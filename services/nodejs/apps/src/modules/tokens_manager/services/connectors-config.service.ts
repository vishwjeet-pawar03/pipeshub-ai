import {
  ConfigurationManagerCommandOptions,
  ConfigurationManagerResponse,
  ConfigurationManagerServiceCommand,
} from '../../../libs/commands/configuration_manager/cm.service.command';
import { HttpMethod } from '../../../libs/enums/http-methods.enum';
import { NotFoundError } from '../../../libs/errors/http.errors';
import {
  AuthenticatedUserRequest,
  ScopedTokenRequest,
} from '../../../libs/middlewares/types';
import {
  GOOGLE_WORKSPACE_BUSINESS_CREDENTIALS_PATH,
  GOOGLE_WORKSPACE_CONFIG_PATH,
  GOOGLE_WORKSPACE_CREDENTIALS_PATH,
  GOOGLE_WORKSPACE_INDIVIDUAL_CREDENTIALS_PATH,
} from '../consts/constants';
import { generateFetchConfigToken } from '../utils/generateToken';

export const getGoogleWorkspaceBusinessCredentials = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }

  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_BUSINESS_CREDENTIALS_PATH}`,
      method: HttpMethod.GET,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const getGoogleWorkspaceIndividualCredentials = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }

  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_INDIVIDUAL_CREDENTIALS_PATH}`,
      method: HttpMethod.GET,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const setGoogleWorkspaceBusinessCredentials = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }

  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_CREDENTIALS_PATH}`,
      method: HttpMethod.POST,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
      body: req.body,
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const deleteGoogleWorkspaceCredentials = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }

  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_BUSINESS_CREDENTIALS_PATH}`,
      method: HttpMethod.DELETE,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const getGoogleWorkspaceConfig = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }
  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_CONFIG_PATH}`,
      method: HttpMethod.GET,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const setGoogleWorkspaceConfig = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }
  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_CONFIG_PATH}`,
      method: HttpMethod.POST,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
      body: req.body,
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const setGoogleWorkspaceIndividualCredentials = async (
  req: AuthenticatedUserRequest,
  url: string,
  scopedJwtSecret: string,
  access_token: string,
  refresh_token: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.user) {
    throw new NotFoundError('User Not Found');
  }
  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_CREDENTIALS_PATH}`,
      method: HttpMethod.POST,
      headers: {
        Authorization: `Bearer ${await generateFetchConfigToken(req.user, scopedJwtSecret)}`,
        'Content-Type': 'application/json',
      },
      body: {
        access_token,
        refresh_token,
      },
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const getRefreshTokenCredentials = async (
  req: ScopedTokenRequest,
  url: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.tokenPayload) {
    throw new NotFoundError('User Not Found');
  }
  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_INDIVIDUAL_CREDENTIALS_PATH}`,
      method: HttpMethod.GET,
      headers: req.headers as Record<string, string>,
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const getRefreshTokenConfig = async (
  req: ScopedTokenRequest,
  url: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.tokenPayload) {
    throw new NotFoundError('User Not Found');
  }
  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_CONFIG_PATH}`,
      method: HttpMethod.GET,
      headers: req.headers as Record<string, string>,
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};

export const setRefreshTokenCredentials = async (
  req: ScopedTokenRequest,
  url: string,
  access_token: string,
  refresh_token: string,
): Promise<ConfigurationManagerResponse> => {
  if (!req.tokenPayload) {
    throw new NotFoundError('User Not Found');
  }
  const configurationManagerCommandOptions: ConfigurationManagerCommandOptions =
    {
      uri: `${url}/${GOOGLE_WORKSPACE_CREDENTIALS_PATH}`,
      method: HttpMethod.POST,
      headers: req.headers as Record<string, string>,
      body: {
        access_token,
        refresh_token,
      },
    };

  const cmCommand = new ConfigurationManagerServiceCommand(
    configurationManagerCommandOptions,
  );
  const response = await cmCommand.execute();
  return response;
};
