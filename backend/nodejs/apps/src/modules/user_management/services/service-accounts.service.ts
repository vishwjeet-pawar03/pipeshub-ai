import { injectable, inject } from 'inversify';
import mongoose from 'mongoose';
import { Logger } from '../../../libs/services/logger.service';
import {
  BadRequestError,
  ConflictError,
  NotFoundError,
} from '../../../libs/errors/http.errors';
import { Users, User } from '../schema/users.schema';
import { UserGroups } from '../schema/userGroup.schema';
import {
  EntitiesEventProducer,
  Event,
  EventType,
  SyncAction,
  UserAddedEvent,
  UserDeletedEvent,
  UserUpdatedEvent,
} from './entity_events.service';
import {
  SERVICE_ACCOUNT_ROLE,
  buildServiceAccountEmail,
  isValidServiceAccountSlug,
  serviceAccountSlugFromEmail,
  SERVICE_ACCOUNT_SLUG_MIN_LENGTH,
  SERVICE_ACCOUNT_SLUG_MAX_LENGTH,
} from '../constants/service-account.constants';

export interface ServiceAccountView {
  id: string;
  slug: string;
  fullName: string;
  email: string;
  description?: string;
  isDisabled: boolean;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface CreateServiceAccountInput {
  slug: string;
  fullName: string;
  description?: string;
}

export interface UpdateServiceAccountInput {
  fullName?: string;
  description?: string;
  isDisabled?: boolean;
}

/**
 * Mongoose types `_id` loosely enough that stringifying it directly trips the
 * lint rule guarding against `[object Object]` creeping into output. Going
 * through ObjectId keeps that guarantee visible in one place.
 */
function idOf(doc: { _id?: unknown }): string {
  return (doc._id as mongoose.Types.ObjectId).toString();
}

function toView(
  user: User & { createdAt?: Date; updatedAt?: Date },
  orgId: string,
): ServiceAccountView {
  return {
    id: idOf(user),
    slug: serviceAccountSlugFromEmail(user.email, orgId),
    fullName: user.fullName ?? '',
    email: user.email,
    description: user.description,
    isDisabled: user.isDisabled ?? false,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
  };
}

/**
 * Service accounts: machine identities that automation authenticates as.
 *
 * The important design decision is that a service account is an ordinary user
 * record with `kind: 'service'`, rather than a new sort of principal. That is
 * what lets it reach the permission graph through the path human users already
 * take, and it is why "what can this service account see" is answered by the
 * same code that answers the question for people. A parallel notion of access
 * would be a second thing to keep correct, and the two would drift.
 */
@injectable()
export class ServiceAccountsService {
  constructor(
    @inject('Logger') private readonly logger: Logger,
    @inject('EntitiesEventProducer')
    private readonly eventService: EntitiesEventProducer,
  ) {}

  async create(
    orgId: string,
    input: CreateServiceAccountInput,
  ): Promise<ServiceAccountView> {
    const slug = input.slug.trim().toLowerCase();
    if (!isValidServiceAccountSlug(slug)) {
      throw new BadRequestError(
        `Name must be ${String(SERVICE_ACCOUNT_SLUG_MIN_LENGTH)}-${String(SERVICE_ACCOUNT_SLUG_MAX_LENGTH)} characters of lowercase letters, digits and single hyphens, and may not start or end with a hyphen`,
      );
    }

    const email = buildServiceAccountEmail(slug, orgId);

    // Email is unique across the whole collection, so a duplicate would fail
    // at the index anyway. Checking first turns a driver error into a message
    // that says which name is taken.
    const existing = await Users.findOne({ email, isDeleted: false })
      .select('_id')
      .lean()
      .exec();
    if (existing) {
      throw new ConflictError(
        `A service account named "${slug}" already exists`,
      );
    }

    const serviceAccount = new Users({
      orgId: new mongoose.Types.ObjectId(orgId),
      email,
      fullName: input.fullName.trim(),
      description: input.description?.trim(),
      kind: 'service',
      // Never an admin, whoever creates it. A service account that could hold
      // admin rights would be a way to obtain them without a person attached.
      role: SERVICE_ACCOUNT_ROLE,
      isDisabled: false,
    });

    // Saved before the event goes out: the consumer builds a permission-graph
    // node from it, and a node for a record that failed to save would be a
    // principal with access and no way to administer it.
    await serviceAccount.save();

    await UserGroups.updateOne(
      { orgId: serviceAccount.orgId, type: 'everyone' },
      { $addToSet: { users: serviceAccount._id } },
    );

    const addedPayload: UserAddedEvent = {
      orgId,
      userId: idOf(serviceAccount),
      fullName: serviceAccount.fullName,
      email: serviceAccount.email,
      syncAction: SyncAction.Immediate,
    };
    await this.publish({
      eventType: EventType.NewUserEvent,
      timestamp: Date.now(),
      payload: addedPayload,
    });

    this.logger.info('Service account created', {
      orgId,
      serviceAccountId: idOf(serviceAccount),
      slug,
    });

    return toView(serviceAccount, orgId);
  }

  async list(orgId: string): Promise<ServiceAccountView[]> {
    const accounts = await Users.find({
      orgId,
      kind: 'service',
      isDeleted: false,
    })
      .sort({ createdAt: -1 })
      .exec();
    return accounts.map((account) => toView(account, orgId));
  }

  async get(orgId: string, id: string): Promise<ServiceAccountView> {
    return toView(await this.findOrThrow(orgId, id), orgId);
  }

  async update(
    orgId: string,
    id: string,
    input: UpdateServiceAccountInput,
  ): Promise<ServiceAccountView> {
    const account = await this.findOrThrow(orgId, id);

    if (input.fullName !== undefined) account.fullName = input.fullName.trim();
    if (input.description !== undefined) {
      account.description = input.description.trim();
    }
    if (input.isDisabled !== undefined) account.isDisabled = input.isDisabled;

    await account.save();

    // The graph keeps its own copy of the display name, so a rename has to
    // reach it too or search results will go on showing the old one.
    if (input.fullName !== undefined) {
      const updatedPayload: UserUpdatedEvent = {
        orgId,
        userId: idOf(account),
        fullName: account.fullName,
        email: account.email,
      };
      await this.publish({
        eventType: EventType.UpdateUserEvent,
        timestamp: Date.now(),
        payload: updatedPayload,
      });
    }

    this.logger.info('Service account updated', {
      orgId,
      serviceAccountId: id,
      disabled: account.isDisabled,
    });

    return toView(account, orgId);
  }

  async remove(orgId: string, id: string): Promise<void> {
    const account = await this.findOrThrow(orgId, id);

    account.isDeleted = true;
    await account.save();

    await UserGroups.updateMany(
      { orgId: account.orgId },
      { $pull: { users: account._id } },
    );

    const deletedPayload: UserDeletedEvent = {
      orgId,
      userId: idOf(account),
      email: account.email,
    };
    await this.publish({
      eventType: EventType.DeleteUserEvent,
      timestamp: Date.now(),
      payload: deletedPayload,
    });

    this.logger.info('Service account deleted', {
      orgId,
      serviceAccountId: id,
    });
  }

  /**
   * Looks a service account up within one organisation.
   *
   * `kind: 'service'` is part of the query rather than something checked
   * afterwards, so this can never be used to reach a human user's record by
   * passing their id to a service-account route.
   */
  private async findOrThrow(orgId: string, id: string): Promise<User> {
    if (!mongoose.Types.ObjectId.isValid(id)) {
      throw new NotFoundError('Service account not found');
    }
    const account = await Users.findOne({
      _id: id,
      orgId,
      kind: 'service',
      isDeleted: false,
    }).exec();
    if (!account) {
      throw new NotFoundError('Service account not found');
    }
    return account;
  }

  private async publish(event: Event): Promise<void> {
    await this.eventService.start();
    await this.eventService.publishEvent(event);
    await this.eventService.stop();
  }
}
