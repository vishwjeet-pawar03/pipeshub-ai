import { Document, Schema, Types, Model } from 'mongoose';
import mongoose from 'mongoose';
import { jurisdictions } from '../../../libs/utils/juridiction.utils';

import { Address } from '../../../libs/utils/address.utils';
import { generateUniqueSlug } from '../../../libs/utils/counter';

export const userRoles = ['admin', 'member'] as const;
export type UserRole = (typeof userRoles)[number];

/**
 * What sort of principal this record represents. `human` is someone who signs
 * in; `service` is a machine identity that automation authenticates as and
 * that can never sign in interactively. Everything that already existed is a
 * human, which is why that is the default and why no migration is needed.
 */
export const userKinds = ['human', 'service'] as const;
export type UserKind = (typeof userKinds)[number];

export interface User extends Document, Address {
  slug?: string;
  orgId: Types.ObjectId;
  fullName?: string;
  firstName?: string;
  lastName?: string;
  middleName?: string;
  email: string;
  mobile?: string;
  hasLoggedIn?: boolean;
  designation?: string;
  kind?: UserKind;
  /** Free text explaining what a service account is for. Unused for humans. */
  description?: string;
  /**
   * Suspends the account without deleting it: tokens stop working and no
   * session can be issued, but the record, its group memberships and its
   * permission-graph node all survive so it can be switched back on.
   */
  isDisabled?: boolean;
  /** Org privilege: admin | member (replaces membership in type=admin UserGroup) */
  role?: UserRole;
  address?: Address;
  isDeleted?: boolean;
  deletedBy?: string;
}

const userSchema = new Schema<User>(
  {
    slug: { type: String, unique: true },
    orgId: { type: Schema.Types.ObjectId, ref: 'orgs', required: true },
    fullName: { type: String, trim: true },
    firstName: { type: String, trim: true },
    lastName: { type: String, trim: true },
    middleName: { type: String, trim: true },
    email: {
      type: String,
      required: [true, 'Email required'],
      lowercase: true,
      unique: true,
    },
    mobile: { type: String },
    hasLoggedIn: { type: Boolean, default: false },
    designation: { type: String, trim: true },
    kind: {
      type: String,
      enum: userKinds,
      default: 'human',
      index: true,
    },
    description: { type: String, trim: true },
    isDisabled: { type: Boolean, default: false },
    role: {
      type: String,
      enum: userRoles,
      default: 'member',
    },
    address: {
      type: {
        addressLine1: { type: String },
        city: { type: String },
        state: { type: String },
        postCode: { type: String },
        country: { type: String, enum: Object.values(jurisdictions) },
      },
    },
    isDeleted: { type: Boolean, default: false },
    deletedBy: { type: String },
  },
  { timestamps: true },
);

userSchema.pre<User>('save', async function (next) {
  try {
    if (!this.slug) {
      this.slug = await generateUniqueSlug('User');
    }
    next();
  } catch (error) {
    next(error as Error);
  }
});

export const Users: Model<User> =
  (mongoose.models['users'] as Model<User>) ||
  mongoose.model<User>('users', userSchema, 'users');
