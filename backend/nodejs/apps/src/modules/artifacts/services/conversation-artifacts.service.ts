import { HttpMethod } from '../../../libs/enums/http-methods.enum';
import {
  executeConnectorCommand,
  handleBackendError,
} from '../../tokens_manager/utils/connector.utils';

/**
 * Deletes every artifact the caller owns in a conversation, and the files the
 * assistant stored for it, before the conversation itself is deleted. Throws
 * when that could not be done, so the conversation is not deleted and the
 * user's retry finishes the cleanup.
 *
 * Only the caller's own token is sent: the connectors service deletes nothing
 * the caller does not own.
 */
export async function deleteConversationArtifacts(
  connectorBackend: string,
  conversationId: string,
  headers: Record<string, unknown>,
): Promise<void> {
  const operation = 'delete conversation artifacts';
  try {
    const response = await executeConnectorCommand(
      `${connectorBackend}/api/v1/artifacts/conversations/${encodeURIComponent(conversationId)}`,
      HttpMethod.DELETE,
      {
        authorization:
          typeof headers.authorization === 'string'
            ? headers.authorization
            : '',
      },
    );
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw handleBackendError(response, operation);
    }
  } catch (error: unknown) {
    throw handleBackendError(error, operation);
  }
}
