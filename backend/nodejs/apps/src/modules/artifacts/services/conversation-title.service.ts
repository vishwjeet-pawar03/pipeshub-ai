import { Types } from 'mongoose';
import { ChatSession } from '../../enterprise_search/schema/chat.session.schema';
import { ProjectService } from '../../projects/services/project.service';

/** What the artifacts gallery shows about an artifact's conversation. */
export interface ConversationSummary {
  title: string;
  /** Set for an agent chat, so the gallery can open it under its agent. */
  agentKey?: string;
  /** Set for a chat in a project the caller can view, so it opens inside that project. */
  projectId?: string;
  /** Set when the chat is archived; it then opens from the archived chats page. */
  conversationArchived?: true;
}

/**
 * Display-only conversation titles for the artifacts gallery.
 * Authorization for the files themselves lives on the graph record ACL;
 * this join never grants access and omits titles the caller cannot see.
 *
 * The access predicate matches `buildFilter`: owner, a chat that is both
 * `isShared` and listed in `sharedWith` for this user, or a project-visible
 * chat in a project the caller can view. `isShared` alone is not enough —
 * unshare removes the viewer from `sharedWith` while leaving the chat shared
 * with others.
 */
export class ConversationTitleService {
  static async batchTitles(
    conversationIds: string[],
    orgId: string,
    userId: string,
  ): Promise<Map<string, ConversationSummary>> {
    const uniqueIds = [...new Set(conversationIds.filter(Boolean))];
    const objectIds = uniqueIds
      .filter((id) => Types.ObjectId.isValid(id))
      .map((id) => new Types.ObjectId(id));
    if (!objectIds.length) {
      return new Map();
    }

    const userObjectId = new Types.ObjectId(userId);
    const accessibleProjectIds = await ProjectService.getAccessibleProjectIds(
      orgId,
      userId,
    );

    const sessions = await ChatSession.find(
      {
        _id: { $in: objectIds },
        orgId: new Types.ObjectId(orgId),
        isDeleted: { $ne: true },
        $or: [
          { userId: userObjectId },
          {
            $and: [{ isShared: true }, { 'sharedWith.userId': userObjectId }],
          },
          ...(accessibleProjectIds.length > 0
            ? [
                {
                  projectId: { $in: accessibleProjectIds },
                  projectVisibility: 'project' as const,
                },
              ]
            : []),
        ],
      },
      { _id: 1, title: 1, sessionType: 1, agentKey: 1, projectId: 1, isArchived: 1 },
    ).lean();

    const accessibleProjects = new Set(accessibleProjectIds.map(String));
    return new Map(
      (sessions || []).map((session) => [
        String(session._id),
        {
          title: session.title ?? 'Untitled',
          ...(session.sessionType === 'agent' && session.agentKey
            ? { agentKey: session.agentKey }
            : {}),
          ...(session.projectId && accessibleProjects.has(String(session.projectId))
            ? { projectId: String(session.projectId) }
            : {}),
          ...(session.isArchived ? { conversationArchived: true as const } : {}),
        },
      ]),
    );
  }
}
