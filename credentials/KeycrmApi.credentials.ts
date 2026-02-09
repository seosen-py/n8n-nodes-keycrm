import type {
	IAuthenticateGeneric,
	ICredentialTestRequest,
	ICredentialType,
	Icon,
	INodeProperties,
} from 'n8n-workflow';

export class KeycrmApi implements ICredentialType {
	name = 'keycrmApi';

	displayName = 'KeyCRM API';

	icon: Icon = {
		light: 'file:../nodes/Keycrm/keycrm.svg',
		dark: 'file:../nodes/Keycrm/keycrm.dark.svg',
	};

	documentationUrl = 'https://help.keycrm.app/uk/api';

	properties: INodeProperties[] = [
		{
			displayName: 'API Token',
			name: 'apiToken',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				Authorization:
					'={{$credentials.apiToken && $credentials.apiToken.trim().toLowerCase().startsWith("bearer ") ? $credentials.apiToken.trim() : "Bearer " + $credentials.apiToken.trim()}}',
			},
		},
	};

	test: ICredentialTestRequest = {
		request: {
			baseURL: 'https://openapi.keycrm.app/v1',
			url: '/users',
			method: 'GET',
		},
	};
}
