import type {
	IDataObject,
	IExecuteFunctions,
	IHttpRequestMethods,
	IHttpRequestOptions,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeProperties,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { ApplicationError, NodeConnectionTypes, NodeOperationError } from 'n8n-workflow';

import openApiData from './openapi-data.json';

declare const Buffer: {
	from(value: string, encoding: string): Uint8Array;
};

declare class Blob {
	constructor(parts?: unknown[], options?: { type?: string });
}

declare class FormData {
	append(name: string, value: unknown, fileName?: string): void;
}

type PrimitiveSchemaType = 'string' | 'number' | 'integer' | 'boolean';
type QueryFilterFieldType = 'string' | 'number' | 'integer' | 'boolean' | 'betweenDateTime';

interface IUiOption {
	name: string;
	value: string;
	description?: string;
}

interface IPathUiField {
	name: string;
	apiPath: string;
	label: string;
	description?: string;
	required: boolean;
	example?: unknown;
}

interface IQuerySimpleField {
	name: string;
	apiPath: string;
	label: string;
	description?: string;
	required: boolean;
	schemaType: PrimitiveSchemaType;
	format?: string;
	enumValues: string[];
	minimum?: number;
	maximum?: number;
	default?: unknown;
	example?: unknown;
}

interface IQueryIncludeField {
	name: string;
	label: string;
	description?: string;
	options: IUiOption[];
}

interface IQuerySortField {
	name: string;
	label: string;
	description?: string;
	options: IUiOption[];
}

interface IQueryFilterField {
	name: string;
	label: string;
	description?: string;
	fieldType: QueryFilterFieldType;
	example?: unknown;
}

interface IQueryUi {
	simple: IQuerySimpleField[];
	include: IQueryIncludeField | null;
	sort: IQuerySortField | null;
	filters: IQueryFilterField[];
}

interface IBodyPrimitiveField {
	kind: 'primitive';
	apiKey: string;
	apiPath: string;
	label: string;
	description?: string;
	required: boolean;
	nullable: boolean;
	schemaType: PrimitiveSchemaType;
	format?: string;
	enumValues: string[];
	example?: unknown;
	default?: unknown;
}

interface IBodyObjectField {
	kind: 'object';
	apiKey: string;
	apiPath: string;
	label: string;
	description?: string;
	required: boolean;
	nullable: boolean;
	children: IBodyField[];
}

interface IBodyArrayField {
	kind: 'array';
	apiKey: string;
	apiPath: string;
	label: string;
	description?: string;
	required: boolean;
	nullable: boolean;
	itemField: IBodyField;
}

type IBodyField = IBodyPrimitiveField | IBodyObjectField | IBodyArrayField;

interface IBodyUi {
	contentType: string;
	binaryProperty: string | null;
	requiredFields: IBodyField[];
	optionalFields: IBodyField[];
}

interface IOperationMeta {
	operationValue: string;
	operationId: string;
	operationLabel: string;
	method: string;
	path: string;
	summary: string;
	description: string;
	pathUi: IPathUiField[];
	queryUi: IQueryUi;
	bodyUi: IBodyUi | null;
}

interface IResourceMeta {
	resourceValue: string;
	resourceLabel: string;
	operations: IOperationMeta[];
}

interface IOpenApiMetadata {
	resources: IResourceMeta[];
	operationCount: number;
}

const KEYCRM_CREDENTIAL_NAME = 'keycrmApi';
const KEYCRM_BASE_URL = 'https://openapi.keycrm.app/v1';
const METADATA = openApiData as IOpenApiMetadata;

function sanitizeParamToken(value: string): string {
	const sanitized = value.replace(/[^a-zA-Z0-9]+/g, '__').replace(/^_+|_+$/g, '').toLowerCase();
	return sanitized || 'value';
}

function operationToken(operation: IOperationMeta): string {
	return sanitizeParamToken(operation.operationId);
}

function pathParamName(operation: IOperationMeta, field: IPathUiField): string {
	return `path__${operationToken(operation)}__${sanitizeParamToken(field.apiPath || field.name)}`;
}

function querySimpleParamName(operation: IOperationMeta, field: IQuerySimpleField): string {
	return `query__${operationToken(operation)}__${sanitizeParamToken(field.apiPath || field.name)}`;
}

function queryIncludeParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__include`;
}

function querySortParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__sort`;
}

function queryGetAllParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__get_all`;
}

function queryOptionsCollectionParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__options`;
}

function queryPageOptionParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__options__page`;
}

function queryCustomFieldsFilterParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__filter__custom_fields`;
}

function queryFilterCollectionParamName(operation: IOperationMeta): string {
	return `query__${operationToken(operation)}__filter`;
}

function queryFilterFieldParamName(operation: IOperationMeta, field: IQueryFilterField): string {
	return `query__${operationToken(operation)}__filter__${sanitizeParamToken(field.name)}`;
}

function queryFilterFromParamName(operation: IOperationMeta, field: IQueryFilterField): string {
	return `${queryFilterFieldParamName(operation, field)}__from`;
}

function queryFilterToParamName(operation: IOperationMeta, field: IQueryFilterField): string {
	return `${queryFilterFieldParamName(operation, field)}__to`;
}

function bodyFieldParamName(operation: IOperationMeta, field: IBodyField): string {
	return `body__${operationToken(operation)}__${sanitizeParamToken(field.apiPath || field.apiKey)}`;
}

function bodyOptionalCollectionParamName(operation: IOperationMeta): string {
	return `body__${operationToken(operation)}__optional`;
}

function bodyBinaryPropertyParamName(operation: IOperationMeta): string {
	return `body__${operationToken(operation)}__binary_property`;
}

function buildApiFieldDescription(description: string | undefined, apiPath: string): string {
	const normalized = description?.trim();
	if (normalized) {
		return `${normalized}\nAPI field: ${apiPath}`;
	}
	return `API field: ${apiPath}`;
}

function operationDisplayOptions(
	operation: IOperationMeta & { resourceValue: string },
): INodeProperties['displayOptions'] {
	return {
		show: {
			resource: [operation.resourceValue],
			operation: [operation.operationValue],
		},
	};
}

function getCustomFieldModelForResource(resourceValue: string): string | undefined {
	const mapping: Record<string, string> = {
		buyer: 'client',
		company: 'client',
		order: 'order',
		pipelines: 'lead',
		products: 'crm_product',
	};
	return mapping[resourceValue];
}

function isCustomFieldUuidBodyField(field: IBodyPrimitiveField): boolean {
	return field.apiKey === 'uuid' && field.apiPath.includes('custom_fields');
}

function isObject(value: unknown): value is IDataObject {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isEmptyValue(value: unknown): boolean {
	if (value === undefined || value === null) {
		return true;
	}
	if (typeof value === 'string') {
		return value.trim() === '';
	}
	if (Array.isArray(value)) {
		return value.length === 0;
	}
	if (isObject(value)) {
		return Object.keys(value).length === 0;
	}
	return false;
}

function padDatePart(value: number): string {
	return value.toString().padStart(2, '0');
}

function toKeycrmUtcDateTime(value: string): string {
	const parsed = new Date(value);
	if (Number.isNaN(parsed.getTime())) {
		throw new ApplicationError(`Invalid date value: ${value}`);
	}
	return `${parsed.getUTCFullYear()}-${padDatePart(parsed.getUTCMonth() + 1)}-${padDatePart(
		parsed.getUTCDate(),
	)} ${padDatePart(parsed.getUTCHours())}:${padDatePart(parsed.getUTCMinutes())}:${padDatePart(
		parsed.getUTCSeconds(),
	)}`;
}

function normalizePrimitiveValue(field: IBodyPrimitiveField, value: unknown): unknown {
	if (field.schemaType === 'number' || field.schemaType === 'integer') {
		if (isEmptyValue(value)) {
			return undefined;
		}
		const parsed = Number(value);
		if (Number.isNaN(parsed)) {
			return undefined;
		}
		if (field.schemaType === 'integer') {
			return Math.trunc(parsed);
		}
		return parsed;
	}

	if (field.schemaType === 'boolean') {
		if (typeof value === 'boolean') {
			return value;
		}
		if (typeof value === 'string') {
			const lowered = value.trim().toLowerCase();
			if (lowered === 'true') {
				return true;
			}
			if (lowered === 'false') {
				return false;
			}
		}
		return undefined;
	}

	if (value === null || value === undefined) {
		return undefined;
	}
	const stringValue = String(value);
	if (!field.nullable && stringValue.trim() === '') {
		return undefined;
	}
	return stringValue;
}

function normalizeSimpleQueryValue(field: IQuerySimpleField, value: unknown): unknown {
	if (field.schemaType === 'number' || field.schemaType === 'integer') {
		const parsed = Number(value);
		if (Number.isNaN(parsed)) {
			return undefined;
		}
		return field.schemaType === 'integer' ? Math.trunc(parsed) : parsed;
	}

	if (field.schemaType === 'boolean') {
		return Boolean(value);
	}

	return String(value);
}

function toDataObjectArray(values: unknown[]): IDataObject[] {
	const output: IDataObject[] = [];
	for (const value of values) {
		if (isObject(value)) {
			output.push(value);
			continue;
		}
		output.push({ value: value as never });
	}
	return output;
}

function extractPaginatedItems(response: unknown): IDataObject[] {
	if (Array.isArray(response)) {
		return toDataObjectArray(response);
	}
	if (!isObject(response)) {
		return [];
	}
	const data = response.data;
	if (Array.isArray(data)) {
		return toDataObjectArray(data);
	}
	const items = response.items;
	if (Array.isArray(items)) {
		return toDataObjectArray(items);
	}
	return [];
}

function hasMorePaginatedItems(
	response: unknown,
	page: number,
	pageSize: number,
	itemsCount: number,
): boolean {
	if (isObject(response)) {
		const currentPage = Number(response.current_page);
		const lastPage = Number(response.last_page);
		if (!Number.isNaN(currentPage) && !Number.isNaN(lastPage) && lastPage > 0) {
			return currentPage < lastPage;
		}

		const metaPage = Number(response.page);
		const metaLastPage = Number(response.last_page);
		if (!Number.isNaN(metaPage) && !Number.isNaN(metaLastPage) && metaLastPage > 0) {
			return metaPage < metaLastPage;
		}

		if (response.next_page_url !== undefined) {
			return Boolean(response.next_page_url);
		}
		const links = response.links;
		if (isObject(links) && links.next !== undefined) {
			return Boolean(links.next);
		}
	}

	return itemsCount >= pageSize && page < 1000;
}

function resolvePageSize(limitField: IQuerySimpleField): number {
	const candidates = [limitField.maximum, limitField.default, limitField.example];
	for (const candidate of candidates) {
		const parsed = Number(candidate);
		if (!Number.isNaN(parsed) && parsed > 0) {
			return Math.trunc(parsed);
		}
	}
	return 50;
}

function createPrimitiveNodeProperty(
	field: IBodyPrimitiveField,
	name: string,
	required: boolean,
	displayOptions?: INodeProperties['displayOptions'],
): INodeProperties {
	const description = buildApiFieldDescription(field.description, field.apiPath);

	if (isCustomFieldUuidBodyField(field)) {
		const property: INodeProperties = {
			displayName: 'Custom Field Name or ID',
			name,
			type: 'options',
			description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
			default: '',
			typeOptions: {
				loadOptionsMethod: 'getCustomFields',
			},
			options: [],
		};
		if (required) {
			property.required = true;
		}
		if (displayOptions) {
			property.displayOptions = displayOptions;
		}
		return property;
	}

	if (field.enumValues.length > 0) {
		const property: INodeProperties = {
			displayName: field.label,
			name,
			type: 'options',
			default: '',
			description,
			options: field.enumValues.map((value) => ({ name: value, value })),
		};
		if (required) {
			property.required = true;
		}
		if (displayOptions) {
			property.displayOptions = displayOptions;
		}
		return property;
	}

	if (field.schemaType === 'number' || field.schemaType === 'integer') {
		const property: INodeProperties = {
			displayName: field.label,
			name,
			type: 'number',
			default: 0,
			description,
		};
		if (required) {
			property.required = true;
		}
		if (displayOptions) {
			property.displayOptions = displayOptions;
		}
		return property;
	}

	if (field.schemaType === 'boolean') {
		const property: INodeProperties = {
			displayName: field.label,
			name,
			type: 'boolean',
			default: false,
			description,
		};
		if (required) {
			property.required = true;
		}
		if (displayOptions) {
			property.displayOptions = displayOptions;
		}
		return property;
	}

	const property: INodeProperties = {
		displayName: field.label,
		name,
		type: field.format === 'date-time' ? 'dateTime' : 'string',
		default: '',
		description,
	};
	if (required) {
		property.required = true;
	}
	if (displayOptions) {
		property.displayOptions = displayOptions;
	}
	return property;
}

function createBodyFieldNodeProperty(
	operation: IOperationMeta,
	field: IBodyField,
	required: boolean,
	displayOptions?: INodeProperties['displayOptions'],
): INodeProperties {
	const fieldName = bodyFieldParamName(operation, field);

	if (field.kind === 'primitive') {
		return createPrimitiveNodeProperty(field, fieldName, required, displayOptions);
	}

	if (field.kind === 'object') {
		const property: INodeProperties = {
			displayName: field.label,
			name: fieldName,
			type: 'collection',
			default: {},
			description: buildApiFieldDescription(field.description, field.apiPath),
			options: field.children.map((child) =>
				createBodyFieldNodeProperty(operation, child, child.required, undefined),
			),
		};
		if (required) {
			property.required = true;
		}
		if (displayOptions) {
			property.displayOptions = displayOptions;
		}
		return property;
	}

	const itemField = field.itemField;
	let itemValues: INodeProperties[] = [];
	if (itemField.kind === 'primitive') {
		itemValues = [createPrimitiveNodeProperty(itemField, bodyFieldParamName(operation, itemField), true)];
	} else if (itemField.kind === 'object') {
		itemValues = itemField.children.map((child) =>
			createBodyFieldNodeProperty(operation, child, child.required, undefined),
		);
	} else {
		itemValues = [
			createBodyFieldNodeProperty(operation, itemField.itemField, itemField.itemField.required, undefined),
		];
	}

	const property: INodeProperties = {
		displayName: field.label,
		name: fieldName,
		type: 'fixedCollection',
		default: {},
		description: buildApiFieldDescription(field.description, field.apiPath),
		typeOptions: {
			multipleValues: true,
		},
		options: [
			{
				name: 'items',
				displayName: 'Items',
				values: itemValues,
			},
		],
	};
	if (required) {
		property.required = true;
	}
	if (displayOptions) {
		property.displayOptions = displayOptions;
	}
	return property;
}

function getOperationMetadata(
	resources: IResourceMeta[],
	resourceKey: string,
	operation: string,
): IOperationMeta {
	for (const resourceEntry of resources) {
		if (resourceEntry.resourceValue !== resourceKey) {
			continue;
		}
		for (const operationEntry of resourceEntry.operations) {
			if (operationEntry.operationValue === operation) {
				return {
					...operationEntry,
					resourceValue: resourceEntry.resourceValue,
					resourceLabel: resourceEntry.resourceLabel,
				} as IOperationMeta & { resourceValue: string; resourceLabel: string };
			}
		}
	}
	throw new ApplicationError(`Unknown operation "${operation}" for resource "${resourceKey}"`);
}

function buildNodeProperties(resources: IResourceMeta[]): INodeProperties[] {
	if (resources.length === 0) {
		throw new ApplicationError('No resources found in openapi-data.json');
	}

	const properties: INodeProperties[] = [];

	const resourceOptions = resources.map((resource) => ({
		name: resource.resourceLabel,
		value: resource.resourceValue,
	}));

	const resourceProperty: INodeProperties = {
		displayName: 'Resource',
		name: 'resource',
		type: 'options',
		noDataExpression: true,
		default: 'buyer',
		options: [
			{
				name: 'Buyer',
				value: 'buyer',
			},
		],
	};
	properties.push(resourceProperty);
	(resourceProperty.options as INodePropertyOptions[]).splice(0, 1, ...resourceOptions);

	for (const resource of resources) {
		const operationOptions: INodePropertyOptions[] = resource.operations.map((operation) => ({
			name: operation.operationLabel,
			value: operation.operationValue,
			description: operation.summary || operation.description,
			action: operation.operationLabel,
		}));

		const firstOperationValue =
			resource.operations.length > 0 ? resource.operations[0].operationValue : 'createNewBuyer';

		const operationProperty: INodeProperties = {
			displayName: 'Operation',
			name: 'operation',
			type: 'options',
			noDataExpression: true,
			displayOptions: {
				show: {
					resource: [resource.resourceValue],
				},
			},
			default: 'createNewBuyer',
			options: [
				{
					name: 'Create New Buyer',
					value: 'createNewBuyer',
					action: 'Create new buyer',
				},
			],
		};
		properties.push(operationProperty);
		operationProperty.default = firstOperationValue;
		(operationProperty.options as INodePropertyOptions[]).splice(0, 1, ...operationOptions);
	}

	for (const resource of resources) {
		for (const operation of resource.operations) {
			const operationWithResource = {
				...operation,
				resourceValue: resource.resourceValue,
				resourceLabel: resource.resourceLabel,
			} as IOperationMeta & { resourceValue: string; resourceLabel: string };
			const displayOptions = operationDisplayOptions(operationWithResource);

			for (const pathParameter of operation.pathUi) {
				const property: INodeProperties = {
					displayName: pathParameter.label,
					name: pathParamName(operationWithResource, pathParameter),
					type: 'string',
					default: '',
					description: buildApiFieldDescription(pathParameter.description, pathParameter.name),
					displayOptions,
				};
				if (pathParameter.required) {
					property.required = true;
				}
				properties.push(property);
			}

			const pageField = operation.queryUi.simple.find((field) => field.name === 'page');
			const limitField = operation.queryUi.simple.find((field) => field.name === 'limit');
			const regularSimpleFields = operation.queryUi.simple.filter(
				(field) => field.name !== 'page' && field.name !== 'limit',
			);

			for (const simpleField of regularSimpleFields) {
				const parameterName = querySimpleParamName(operationWithResource, simpleField);
				const description = buildApiFieldDescription(simpleField.description, simpleField.name);

				if (simpleField.enumValues.length > 0) {
					properties.push({
						displayName: simpleField.label,
						name: parameterName,
						type: 'options',
						default: '',
						description,
						options: simpleField.enumValues.map((value) => ({ name: value, value })),
						displayOptions,
					});
					continue;
				}

				if (simpleField.schemaType === 'number' || simpleField.schemaType === 'integer') {
					properties.push({
						displayName: simpleField.label,
						name: parameterName,
						type: 'number',
						default: 0,
						description,
						displayOptions,
					});
					continue;
				}

				if (simpleField.schemaType === 'boolean') {
					properties.push({
						displayName: simpleField.label,
						name: parameterName,
						type: 'boolean',
						default: false,
						description,
						displayOptions,
					});
					continue;
				}

				properties.push({
					displayName: simpleField.label,
					name: parameterName,
					type: simpleField.format === 'date-time' ? 'dateTime' : 'string',
					default: '',
					description,
					displayOptions,
				});
			}

			if (pageField && limitField) {
				const defaultLimit = resolvePageSize(limitField);
				properties.push({
					displayName: 'Get All',
					name: queryGetAllParamName(operationWithResource),
					type: 'boolean',
					default: true,
					description: 'Whether to return all records from all pages',
					displayOptions,
				});

				const pagedDisplayOptions: INodeProperties['displayOptions'] = {
					show: {
						...(displayOptions?.show ?? {}),
						[queryGetAllParamName(operationWithResource)]: [false],
					},
				};

				properties.push({
					displayName: limitField.label,
					name: querySimpleParamName(operationWithResource, limitField),
					type: 'number',
					default: defaultLimit,
					description: buildApiFieldDescription(limitField.description, limitField.name),
					typeOptions: {
						minValue: 1,
					},
					displayOptions: pagedDisplayOptions,
				});

				properties.push({
					displayName: 'Options',
					name: queryOptionsCollectionParamName(operationWithResource),
					type: 'collection',
					default: {},
					placeholder: 'Add option',
					description: 'Additional pagination options',
					options: [
						{
							displayName: pageField.label,
							name: queryPageOptionParamName(operationWithResource),
							type: 'number',
							default: 1,
							description: buildApiFieldDescription(pageField.description, pageField.name),
							typeOptions: {
								minValue: 1,
							},
						},
					],
					displayOptions: pagedDisplayOptions,
				});
			} else if (limitField) {
				const defaultLimit = resolvePageSize(limitField);
				properties.push({
					displayName: limitField.label,
					name: querySimpleParamName(operationWithResource, limitField),
					type: 'number',
					default: defaultLimit,
					description: buildApiFieldDescription(limitField.description, limitField.name),
					typeOptions: {
						minValue: 1,
					},
					displayOptions,
				});
			} else if (pageField) {
				properties.push({
					displayName: 'Options',
					name: queryOptionsCollectionParamName(operationWithResource),
					type: 'collection',
					default: {},
					placeholder: 'Add option',
					description: 'Additional pagination options',
					options: [
						{
							displayName: pageField.label,
							name: queryPageOptionParamName(operationWithResource),
							type: 'number',
							default: 1,
							description: buildApiFieldDescription(pageField.description, pageField.name),
							typeOptions: {
								minValue: 1,
							},
						},
					],
					displayOptions,
				});
			}

			if (operation.queryUi.include && operation.queryUi.include.options.length > 0) {
				properties.push({
					displayName: operation.queryUi.include.label,
					name: queryIncludeParamName(operationWithResource),
					type: 'multiOptions',
					default: [],
					description: buildApiFieldDescription(
						operation.queryUi.include.description,
						operation.queryUi.include.name,
					),
					options: operation.queryUi.include.options,
					displayOptions,
				});
			}

			if (operation.queryUi.sort && operation.queryUi.sort.options.length > 0) {
				properties.push({
					displayName: operation.queryUi.sort.label,
					name: querySortParamName(operationWithResource),
					type: 'options',
					default: '',
					description: buildApiFieldDescription(
						operation.queryUi.sort.description,
						operation.queryUi.sort.name,
					),
					options: operation.queryUi.sort.options,
					displayOptions,
				});
			}

			if (operation.queryUi.filters.length > 0) {
				const filterOptions: INodeProperties[] = [];
				for (const filterField of operation.queryUi.filters) {
					if (filterField.fieldType === 'betweenDateTime') {
						filterOptions.push({
							displayName: `${filterField.label} From`,
							name: queryFilterFromParamName(operationWithResource, filterField),
							type: 'dateTime',
							default: '',
							description: buildApiFieldDescription(
								filterField.description,
								`${filterField.name}.from`,
							),
						});
						filterOptions.push({
							displayName: `${filterField.label} To`,
							name: queryFilterToParamName(operationWithResource, filterField),
							type: 'dateTime',
							default: '',
							description: buildApiFieldDescription(filterField.description, `${filterField.name}.to`),
						});
						continue;
					}

					if (filterField.fieldType === 'number' || filterField.fieldType === 'integer') {
						filterOptions.push({
							displayName: filterField.label,
							name: queryFilterFieldParamName(operationWithResource, filterField),
							type: 'number',
							default: 0,
							description: buildApiFieldDescription(filterField.description, filterField.name),
						});
						continue;
					}

					if (filterField.fieldType === 'boolean') {
						filterOptions.push({
							displayName: filterField.label,
							name: queryFilterFieldParamName(operationWithResource, filterField),
							type: 'boolean',
							default: false,
							description: buildApiFieldDescription(filterField.description, filterField.name),
						});
						continue;
					}

					filterOptions.push({
						displayName: filterField.label,
						name: queryFilterFieldParamName(operationWithResource, filterField),
						type: 'string',
						default: '',
						description: buildApiFieldDescription(filterField.description, filterField.name),
					});
				}

				if (getCustomFieldModelForResource(operationWithResource.resourceValue)) {
					filterOptions.push({
						displayName: 'Custom Fields',
						name: queryCustomFieldsFilterParamName(operationWithResource),
						type: 'fixedCollection',
						default: {},
						placeholder: 'Add custom field filter',
						description: 'Filter by custom fields',
						typeOptions: {
							multipleValues: true,
						},
						options: [
							{
								name: 'items',
								displayName: 'Custom Field',
								values: [
									{
										displayName: 'Custom Field Name or ID',
										name: 'uuid',
										type: 'options',
										default: '',
										description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
										typeOptions: {
											loadOptionsMethod: 'getCustomFields',
										},
										options: [],
									},
									{
										displayName: 'Value',
										name: 'value',
										type: 'string',
										default: '',
										description: 'Value to filter by',
									},
								],
							},
						],
					});
				}

				properties.push({
					displayName: 'Filter',
					name: queryFilterCollectionParamName(operationWithResource),
					type: 'collection',
					default: {},
					placeholder: 'Add filter',
					description: 'Filtering options',
					options: filterOptions,
					displayOptions,
				});
			}

			if (operation.bodyUi) {
				if (operation.bodyUi.contentType === 'multipart/form-data' && operation.bodyUi.binaryProperty) {
					properties.push({
						displayName: 'Binary Property',
						name: bodyBinaryPropertyParamName(operationWithResource),
						type: 'string',
						default: 'data',
						required: true,
						description: `Input binary property name. File field: ${operation.bodyUi.binaryProperty}.`,
						displayOptions,
					});
				}

				for (const requiredField of operation.bodyUi.requiredFields) {
					properties.push(
						createBodyFieldNodeProperty(
							operationWithResource,
							requiredField,
							true,
							displayOptions,
						),
					);
				}

				if (operation.bodyUi.optionalFields.length > 0) {
					properties.push({
						displayName: 'Additional Fields',
						name: bodyOptionalCollectionParamName(operationWithResource),
						type: 'collection',
						default: {},
						placeholder: 'Add field',
						description: 'Optional body fields',
						options: operation.bodyUi.optionalFields.map((optionalField) =>
							createBodyFieldNodeProperty(operationWithResource, optionalField, false, undefined),
						),
						displayOptions,
					});
				}
			}
		}
	}

	return properties;
}

function shouldIncludeSimpleQueryValue(field: IQuerySimpleField, value: unknown): boolean {
	if (field.required) {
		return true;
	}
	if (isEmptyValue(value)) {
		return false;
	}
	if ((field.schemaType === 'number' || field.schemaType === 'integer') && Number(value) === 0) {
		return field.default !== undefined;
	}
	if (field.schemaType === 'boolean' && value === false) {
		return field.default !== undefined;
	}
	return true;
}

function parseBodyFieldValue(
	field: IBodyField,
	rawValue: unknown,
	itemIndex: number,
	node: IExecuteFunctions,
	operation: IOperationMeta & { resourceValue: string; resourceLabel: string },
): unknown {
	if (field.kind === 'primitive') {
		const normalized = normalizePrimitiveValue(field, rawValue);
		if (field.required && normalized === undefined) {
			throw new NodeOperationError(node.getNode(), `Missing required field "${field.apiPath}"`, {
				itemIndex,
			});
		}
		return normalized;
	}

	if (field.kind === 'object') {
		const source = isObject(rawValue) ? rawValue : {};
		const output: IDataObject = {};

		for (const child of field.children) {
			const childName = bodyFieldParamName(operation, child);
			if (!(childName in source)) {
				if (child.required) {
					throw new NodeOperationError(
						node.getNode(),
						`Missing required field "${child.apiPath}"`,
						{ itemIndex },
					);
				}
				continue;
			}
			const parsedChild = parseBodyFieldValue(
				child,
				source[childName],
				itemIndex,
				node,
				operation,
			);
			if (parsedChild !== undefined) {
				output[child.apiKey] = parsedChild as IDataObject;
			}
		}

		if (Object.keys(output).length === 0) {
			if (field.required) {
				throw new NodeOperationError(node.getNode(), `Missing required object "${field.apiPath}"`, {
					itemIndex,
				});
			}
			return undefined;
		}
		return output;
	}

	const source = isObject(rawValue) ? rawValue : {};
	const rawItems = source.items;
	const items = Array.isArray(rawItems) ? rawItems : [];
	const parsedItems: unknown[] = [];

	for (const itemEntry of items) {
		if (field.itemField.kind === 'primitive') {
			if (!isObject(itemEntry)) {
				continue;
			}
			const itemFieldName = bodyFieldParamName(operation, field.itemField);
			const parsedItem = parseBodyFieldValue(
				field.itemField,
				itemEntry[itemFieldName],
				itemIndex,
				node,
				operation,
			);
			if (parsedItem !== undefined) {
				parsedItems.push(parsedItem);
			}
			continue;
		}

		const parsedItem = parseBodyFieldValue(field.itemField, itemEntry, itemIndex, node, operation);
		if (parsedItem !== undefined) {
			parsedItems.push(parsedItem);
		}
	}

	if (parsedItems.length === 0) {
		if (field.required) {
			throw new NodeOperationError(node.getNode(), `Missing required array "${field.apiPath}"`, {
				itemIndex,
			});
		}
		return undefined;
	}

	return parsedItems;
}

function normalizeResponseData(this: IExecuteFunctions, data: unknown): INodeExecutionData[] {
	if (Array.isArray(data)) {
		return this.helpers.returnJsonArray(data as IDataObject[]);
	}
	if (isObject(data)) {
		return [{ json: data }];
	}
	if (data === null || typeof data === 'string' || typeof data === 'number' || typeof data === 'boolean') {
		return [{ json: { value: data } }];
	}
	return [{ json: { value: JSON.stringify(data) } }];
}

function buildKeycrmUrl(path: string): string {
	return `${KEYCRM_BASE_URL}${path.startsWith('/') ? path : `/${path}`}`;
}

const NODE_PROPERTIES = buildNodeProperties(METADATA.resources);

export class Keycrm implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'KeyCRM',
		name: 'keycrm',
		icon: { light: 'file:keycrm.svg', dark: 'file:keycrm.dark.svg' },
		group: ['input'],
		version: 1,
		subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
		description: 'Work with the KeyCRM API',
		defaults: {
			name: 'KeyCRM',
		},
		usableAsTool: true,
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: KEYCRM_CREDENTIAL_NAME,
				required: true,
			},
		],
		requestDefaults: {
			baseURL: KEYCRM_BASE_URL,
			headers: {
				Accept: 'application/json',
				'Content-Type': 'application/json',
			},
		},
		properties: NODE_PROPERTIES,
	};

	methods = {
		loadOptions: {
			async getCustomFields(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const resourceValue = this.getCurrentNodeParameter('resource') as string | undefined;
				const model = resourceValue ? getCustomFieldModelForResource(resourceValue) : undefined;
				if (!model) {
					return [];
				}

				const response = await this.helpers.httpRequestWithAuthentication.call(
					this,
					KEYCRM_CREDENTIAL_NAME,
					{
						url: buildKeycrmUrl('/custom-fields'),
						method: 'GET',
						qs: {
							model,
						},
					},
				);

				const items =
					Array.isArray(response)
						? response
						: isObject(response) && Array.isArray(response.data)
							? response.data
							: [];

				const options: INodePropertyOptions[] = [];
				for (const item of items) {
					if (!isObject(item)) {
						continue;
					}

					const uuid = item.uuid;
					if (isEmptyValue(uuid)) {
						continue;
					}

					const labelSource = item.name ?? item.title ?? item.code ?? uuid;
					options.push({
						name: String(labelSource),
						value: String(uuid),
					});
				}

				options.sort((a, b) => a.name.localeCompare(b.name));
				return options;
			},
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const returnData: INodeExecutionData[] = [];
		const items = this.getInputData();

		for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
			try {
				const resourceKey = this.getNodeParameter('resource', itemIndex) as string;
				const operationValue = this.getNodeParameter('operation', itemIndex) as string;
				const operationMeta = getOperationMetadata(
					METADATA.resources,
					resourceKey,
					operationValue,
				) as IOperationMeta & { resourceValue: string; resourceLabel: string };

				let endpointPath = operationMeta.path;
				for (const pathField of operationMeta.pathUi) {
					const value = this.getNodeParameter(
						pathParamName(operationMeta, pathField),
						itemIndex,
					) as string;
					if (isEmptyValue(value)) {
						if (pathField.required) {
							throw new NodeOperationError(
								this.getNode(),
								`Missing path parameter "${pathField.name}"`,
								{ itemIndex },
							);
						}
						continue;
					}
					endpointPath = endpointPath.replace(`{${pathField.name}}`, encodeURIComponent(value));
				}

				const query: IDataObject = {};
				const pageField = operationMeta.queryUi.simple.find((field) => field.name === 'page');
				const limitField = operationMeta.queryUi.simple.find((field) => field.name === 'limit');
				const regularSimpleFields = operationMeta.queryUi.simple.filter(
					(field) => field.name !== 'page' && field.name !== 'limit',
				);

				for (const simpleField of regularSimpleFields) {
					const value = this.getNodeParameter(
						querySimpleParamName(operationMeta, simpleField),
						itemIndex,
					);
					if (!shouldIncludeSimpleQueryValue(simpleField, value)) {
						continue;
					}
					const normalized = normalizeSimpleQueryValue(simpleField, value);
					if (normalized !== undefined) {
						query[simpleField.name] = normalized as never;
					}
				}

				let useGetAllWithPagination = false;
				const defaultLimit = limitField ? resolvePageSize(limitField) : 50;
				if (pageField && limitField) {
					const getAll = this.getNodeParameter(
						queryGetAllParamName(operationMeta),
						itemIndex,
						true,
					) as boolean;
					useGetAllWithPagination = getAll;

					if (!getAll) {
						const limitValue = this.getNodeParameter(
							querySimpleParamName(operationMeta, limitField),
							itemIndex,
							defaultLimit,
						);
						if (shouldIncludeSimpleQueryValue(limitField, limitValue)) {
							const normalizedLimit = normalizeSimpleQueryValue(limitField, limitValue);
							if (normalizedLimit !== undefined) {
								const parsedLimit = Math.trunc(Number(normalizedLimit));
								if (Number.isNaN(parsedLimit) || parsedLimit < 1) {
									throw new NodeOperationError(
										this.getNode(),
										`Invalid "${limitField.name}" value. Expected a number greater than 0.`,
										{ itemIndex },
									);
								}
								query[limitField.name] = parsedLimit as never;
							}
						}

						const optionsSource = this.getNodeParameter(
							queryOptionsCollectionParamName(operationMeta),
							itemIndex,
							{},
						) as IDataObject;
						const pageValue = optionsSource[queryPageOptionParamName(operationMeta)];
						if (!isEmptyValue(pageValue)) {
							const parsedPage = Math.trunc(Number(pageValue));
							if (Number.isNaN(parsedPage) || parsedPage < 1) {
								throw new NodeOperationError(
									this.getNode(),
									`Invalid "${pageField.name}" value. Expected a number greater than 0.`,
									{ itemIndex },
								);
							}
							query[pageField.name] = parsedPage as never;
						}
					}
				} else {
					if (limitField) {
						const limitValue = this.getNodeParameter(
							querySimpleParamName(operationMeta, limitField),
							itemIndex,
							defaultLimit,
						);
						if (shouldIncludeSimpleQueryValue(limitField, limitValue)) {
							const normalizedLimit = normalizeSimpleQueryValue(limitField, limitValue);
							if (normalizedLimit !== undefined) {
								const parsedLimit = Math.trunc(Number(normalizedLimit));
								if (Number.isNaN(parsedLimit) || parsedLimit < 1) {
									throw new NodeOperationError(
										this.getNode(),
										`Invalid "${limitField.name}" value. Expected a number greater than 0.`,
										{ itemIndex },
									);
								}
								query[limitField.name] = parsedLimit as never;
							}
						}
					}

					if (pageField) {
						const optionsSource = this.getNodeParameter(
							queryOptionsCollectionParamName(operationMeta),
							itemIndex,
							{},
						) as IDataObject;
						const pageValue = optionsSource[queryPageOptionParamName(operationMeta)];
						if (!isEmptyValue(pageValue)) {
							const parsedPage = Math.trunc(Number(pageValue));
							if (Number.isNaN(parsedPage) || parsedPage < 1) {
								throw new NodeOperationError(
									this.getNode(),
									`Invalid "${pageField.name}" value. Expected a number greater than 0.`,
									{ itemIndex },
								);
							}
							query[pageField.name] = parsedPage as never;
						}
					}
				}

				if (operationMeta.queryUi.include && operationMeta.queryUi.include.options.length > 0) {
					const includeValues = this.getNodeParameter(
						queryIncludeParamName(operationMeta),
						itemIndex,
						[],
					) as string[];
					if (includeValues.length > 0) {
						query.include = includeValues.join(',');
					}
				}

				if (operationMeta.queryUi.sort && operationMeta.queryUi.sort.options.length > 0) {
					const sortValue = this.getNodeParameter(
						querySortParamName(operationMeta),
						itemIndex,
						'',
					) as string;
					if (!isEmptyValue(sortValue)) {
						query.sort = sortValue;
					}
				}

				if (operationMeta.queryUi.filters.length > 0) {
					const filterSource = this.getNodeParameter(
						queryFilterCollectionParamName(operationMeta),
						itemIndex,
						{},
					) as IDataObject;
					const filterPayload: IDataObject = {};

					for (const filterField of operationMeta.queryUi.filters) {
						if (filterField.fieldType === 'betweenDateTime') {
							const fromValue = filterSource[queryFilterFromParamName(operationMeta, filterField)];
							const toValue = filterSource[queryFilterToParamName(operationMeta, filterField)];
							const hasFrom = !isEmptyValue(fromValue);
							const hasTo = !isEmptyValue(toValue);
							if (hasFrom !== hasTo) {
								throw new NodeOperationError(
									this.getNode(),
									`Filter "${filterField.name}" expects both "from" and "to"`,
									{ itemIndex },
								);
							}
							if (hasFrom && hasTo) {
								filterPayload[filterField.name] = `${toKeycrmUtcDateTime(
									String(fromValue),
								)}, ${toKeycrmUtcDateTime(String(toValue))}`;
							}
							continue;
						}

						const fieldKey = queryFilterFieldParamName(operationMeta, filterField);
						if (!(fieldKey in filterSource)) {
							continue;
						}
						const rawValue = filterSource[fieldKey];
						if (isEmptyValue(rawValue)) {
							continue;
						}
						if (filterField.fieldType === 'number' || filterField.fieldType === 'integer') {
							const parsed = Number(rawValue);
							if (!Number.isNaN(parsed)) {
								filterPayload[filterField.name] =
									filterField.fieldType === 'integer' ? Math.trunc(parsed) : parsed;
							}
							continue;
						}
						if (filterField.fieldType === 'boolean') {
							filterPayload[filterField.name] = Boolean(rawValue);
							continue;
						}
						filterPayload[filterField.name] = String(rawValue);
					}

					const customFieldsFilterRaw =
						filterSource[queryCustomFieldsFilterParamName(operationMeta)];
					if (isObject(customFieldsFilterRaw) && Array.isArray(customFieldsFilterRaw.items)) {
						const customFieldsPayload: IDataObject = {};
						for (const item of customFieldsFilterRaw.items) {
							if (!isObject(item)) {
								continue;
							}
							const uuid = item.uuid;
							const value = item.value;
							if (isEmptyValue(uuid) || isEmptyValue(value)) {
								continue;
							}
							customFieldsPayload[String(uuid)] = String(value) as never;
						}
						if (Object.keys(customFieldsPayload).length > 0) {
							filterPayload.custom_fields = customFieldsPayload;
						}
					}

					if (Object.keys(filterPayload).length > 0) {
						query.filter = filterPayload;
					}
				}

				const requestOptions: IHttpRequestOptions = {
					url: buildKeycrmUrl(endpointPath),
					method: operationMeta.method as IHttpRequestMethods,
					qs: query,
				};

				if (operationMeta.bodyUi) {
					if (operationMeta.bodyUi.contentType === 'multipart/form-data' && operationMeta.bodyUi.binaryProperty) {
						const binaryPropertyName = this.getNodeParameter(
							bodyBinaryPropertyParamName(operationMeta),
							itemIndex,
						) as string;
						const binaryData = this.helpers.assertBinaryData(itemIndex, binaryPropertyName);
						const binaryBuffer = Buffer.from(binaryData.data, 'base64');
						const formData = new FormData();
						formData.append(
							operationMeta.bodyUi.binaryProperty,
							new Blob([binaryBuffer], {
								type: binaryData.mimeType || 'application/octet-stream',
							}),
							binaryData.fileName || 'file',
						);
						requestOptions.body = formData as unknown as IDataObject;
						requestOptions.headers = {
							Accept: 'application/json',
						};
					} else {
						const bodyPayload: IDataObject = {};

						for (const requiredField of operationMeta.bodyUi.requiredFields) {
							const rawRequiredValue = this.getNodeParameter(
								bodyFieldParamName(operationMeta, requiredField),
								itemIndex,
							);
							const parsed = parseBodyFieldValue(
								requiredField,
								rawRequiredValue,
								itemIndex,
								this,
								operationMeta,
							);
							if (parsed === undefined) {
								throw new NodeOperationError(
									this.getNode(),
									`Missing required body field "${requiredField.apiPath}"`,
									{ itemIndex },
								);
							}
							bodyPayload[requiredField.apiKey] = parsed as IDataObject;
						}

						if (operationMeta.bodyUi.optionalFields.length > 0) {
							const optionalSource = this.getNodeParameter(
								bodyOptionalCollectionParamName(operationMeta),
								itemIndex,
								{},
							) as IDataObject;

							for (const optionalField of operationMeta.bodyUi.optionalFields) {
								const optionalKey = bodyFieldParamName(operationMeta, optionalField);
								if (!(optionalKey in optionalSource)) {
									continue;
								}
								const parsed = parseBodyFieldValue(
									optionalField,
									optionalSource[optionalKey],
									itemIndex,
									this,
									operationMeta,
								);
								if (parsed !== undefined) {
									bodyPayload[optionalField.apiKey] = parsed as IDataObject;
								}
							}
						}

						if (Object.keys(bodyPayload).length > 0) {
							requestOptions.body = bodyPayload;
						}
					}
				}

				if (useGetAllWithPagination && pageField && limitField) {
					const pageSize = resolvePageSize(limitField);
					const aggregatedItems: IDataObject[] = [];
					let page = 1;
					let lastResponse: unknown = undefined;

					while (true) {
						const paginatedRequestOptions: IHttpRequestOptions = {
							...requestOptions,
							qs: {
								...(requestOptions.qs ?? {}),
								[pageField.name]: page,
								[limitField.name]: pageSize,
							},
						};

						const response = await this.helpers.httpRequestWithAuthentication.call(
							this,
							KEYCRM_CREDENTIAL_NAME,
							paginatedRequestOptions,
						);
						lastResponse = response;

						const pageItems = extractPaginatedItems(response);
						if (pageItems.length > 0) {
							aggregatedItems.push(...pageItems);
						}

						if (!hasMorePaginatedItems(response, page, pageSize, pageItems.length)) {
							break;
						}

						page += 1;
					}

					if (aggregatedItems.length > 0) {
						returnData.push(...this.helpers.returnJsonArray(aggregatedItems));
					} else if (lastResponse !== undefined) {
						returnData.push(...normalizeResponseData.call(this, lastResponse));
					}
					continue;
				}

				const response = await this.helpers.httpRequestWithAuthentication.call(
					this,
					KEYCRM_CREDENTIAL_NAME,
					requestOptions,
				);

				returnData.push(...normalizeResponseData.call(this, response));
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: {
							error: error instanceof Error ? error.message : String(error),
						},
						pairedItem: {
							item: itemIndex,
						},
					});
					continue;
				}
				if (error instanceof NodeOperationError) {
					throw error;
				}
				throw new NodeOperationError(this.getNode(), error as Error, { itemIndex });
			}
		}

		return [returnData];
	}
}
