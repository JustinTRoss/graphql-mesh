import { AggregateError, BaseLoaderOptions, inspect } from '@graphql-tools/utils';
import { isScalarType, OperationTypeNode, specifiedDirectives } from 'graphql';
import { dereferenceObject, healJSONSchema, JSONSchema, JSONSchemaObject, referenceJSONSchema } from 'json-machete';
import { KeyValueCache, Logger, MeshPubSub } from '@graphql-mesh/types';
import toJsonSchema from 'to-json-schema';
import { getComposerFromJSONSchema } from './getComposerFromJSONSchema';
import { SchemaComposer } from 'graphql-compose';
import {
  readFileOrUrl,
  getCachedFetch,
  parseInterpolationStrings,
  stringInterpolator,
  jsonFlatStringify,
  DefaultLogger,
} from '@graphql-mesh/utils';
import { resolveDataByUnionInputType } from './resolveDataByUnionInputType';
import urlJoin from 'url-join';
import { stringify as qsStringify } from 'qs';
import { env } from 'process';

export interface JSONSchemaBaseOperationConfig {
  type: OperationTypeNode;
  field: string;
  description?: string;

  requestSchema?: string | JSONSchema;
  requestSample?: any;
  requestTypeName?: string;

  responseSchema?: string | JSONSchema;
  responseSample?: any;
  responseTypeName?: string;

  argTypeMap?: Record<string, string>;
}

export type HTTPMethod = 'GET' | 'HEAD' | 'POST' | 'PUT' | 'DELETE' | 'CONNECT' | 'OPTIONS' | 'TRACE' | 'PATCH';

export interface JSONSchemaHTTPOperationConfig extends JSONSchemaBaseOperationConfig {
  path: string;
  method?: HTTPMethod;

  headers?: Record<string, string>;
}

export interface JSONSchemaPubSubOperationConfig extends JSONSchemaBaseOperationConfig {
  pubsubTopic: string;
}

export type JSONSchemaOperationConfig = JSONSchemaHTTPOperationConfig | JSONSchemaPubSubOperationConfig;

export interface JSONSchemaLoaderOptions extends BaseLoaderOptions {
  baseUrl?: string;
  operationHeaders?: Record<string, string>;
  schemaHeaders?: Record<string, string>;
  operations: JSONSchemaOperationConfig[];
  disableTimestampScalar?: boolean;
  errorMessage?: string;
  logger?: Logger;
  finalSchema?: JSONSchemaObject;
  cache?: KeyValueCache;
  pubsub?: MeshPubSub;
}

export async function JSONSchemaLoader(name: string, options: JSONSchemaLoaderOptions) {
  options.logger = options.logger || new DefaultLogger(name);
  const finalJSONSchema = options.finalSchema || (await buildFinalJSONSchema(options));
  options.logger.debug(`Derefering the bundled JSON Schema`);
  const fullyDeferencedSchema = await dereferenceObject(finalJSONSchema, {
    cwd: options.cwd,
  });
  options.logger.debug(`Generating GraphQL Schema from the bundled JSON Schema`);
  const visitorResult = await getComposerFromJSONSchema(fullyDeferencedSchema as JSONSchema, options.logger);

  const schemaComposer = visitorResult.output as SchemaComposer;

  // graphql-compose doesn't add @defer and @stream to the schema
  specifiedDirectives.forEach(directive => schemaComposer.addDirective(directive));

  addExecutionLogicToComposer(schemaComposer, options);

  return schemaComposer.buildSchema();
}

export async function addExecutionLogicToComposer(schemaComposer: SchemaComposer, options: JSONSchemaLoaderOptions) {
  const fetch = getCachedFetch(options.cache);

  options.logger.debug(`Attaching execution logic to the schema`);
  for (const operationConfig of options.operations) {
    const { httpMethod, rootTypeName, fieldName } = getOperationMetadata(operationConfig);
    const operationLogger = options.logger.child(`${rootTypeName}.${fieldName}`);

    const interpolationStrings: string[] = [...Object.values(options.operationHeaders || {}), options.baseUrl];

    const rootTypeComposer = schemaComposer[rootTypeName];

    const field = rootTypeComposer.getField(fieldName);

    if (isPubSubOperationConfig(operationConfig)) {
      field.description = field.description || `PubSub Topic: ${operationConfig.pubsubTopic}`;
      field.subscribe = (root, args, context, info) => {
        const interpolationData = { root, args, context, info, env };
        const pubsubTopic = stringInterpolator.parse(operationConfig.pubsubTopic, interpolationData);
        operationLogger.debug(`=> Subscribing to pubSubTopic: ${pubsubTopic}`);
        return options.pubsub.asyncIterator(pubsubTopic);
      };
      field.resolve = root => {
        operationLogger.debug(`Received ${inspect(root)} from ${operationConfig.pubsubTopic}`);
        return root;
      };
      interpolationStrings.push(operationConfig.pubsubTopic);
    } else if (operationConfig.path) {
      field.description = field.description || `${operationConfig.method} ${operationConfig.path}`;
      field.resolve = async (root, args, context, info) => {
        operationLogger.debug(`=> Resolving`);
        const interpolationData = { root, args, context, info, env };
        const interpolatedBaseUrl = stringInterpolator.parse(options.baseUrl, interpolationData);
        const interpolatedPath = stringInterpolator.parse(operationConfig.path, interpolationData);
        const fullPath = urlJoin(interpolatedBaseUrl, interpolatedPath);
        const headers = {
          ...options.operationHeaders,
          ...operationConfig?.headers,
        };
        for (const headerName in headers) {
          headers[headerName] = stringInterpolator.parse(headers[headerName], interpolationData);
        }
        const requestInit: RequestInit = {
          method: httpMethod,
          headers,
        };
        const urlObj = new URL(fullPath);
        // Resolve union input
        const input = resolveDataByUnionInputType(args.input, field.args?.input?.type?.getType(), schemaComposer);
        if (input) {
          switch (httpMethod) {
            case 'GET':
            case 'HEAD':
            case 'CONNECT':
            case 'OPTIONS':
            case 'TRACE': {
              const newSearchParams = new URLSearchParams(input);
              newSearchParams.forEach((value, key) => {
                urlObj.searchParams.set(key, value);
              });
              break;
            }
            case 'POST':
            case 'PUT':
            case 'PATCH':
            case 'DELETE': {
              const [, contentType] =
                Object.entries(headers).find(([key]) => key.toLowerCase() === 'content-type') || [];
              if (contentType?.startsWith('application/x-www-form-urlencoded')) {
                requestInit.body = qsStringify(input);
              } else {
                requestInit.body = jsonFlatStringify(input);
              }
              break;
            }
            default:
              throw new Error(`Unknown method ${httpMethod}`);
          }
        }
        operationLogger.debug(`=> Fetching ${urlObj.toString()}=>${inspect(requestInit)}`);
        const response = await fetch(urlObj.toString(), requestInit);
        const responseText = await response.text();
        operationLogger.debug(
          `=> Fetched from ${urlObj.toString()}=>{
              body: ${responseText}
            }`
        );
        const returnType = field.type;
        let responseJson: any;
        try {
          responseJson = JSON.parse(responseText);
        } catch (e) {
          // The result might be defined as scalar
          if (isScalarType(returnType)) {
            operationLogger.debug(` => Return type is not a JSON so returning ${responseText}`);
            return responseText;
          }
          throw responseText;
        }
        const errorMessageTemplate = options.errorMessage || 'message';
        function normalizeError(error: any): Error {
          const errorMessage = stringInterpolator.parse(errorMessageTemplate, error);
          if (typeof error === 'object' && errorMessage) {
            const errorObj = new Error(errorMessage);
            errorObj.stack = null;
            Object.assign(errorObj, error);
            return errorObj;
          } else {
            return error;
          }
        }
        const errors = responseJson.errors || responseJson._errors;
        // Make sure the return type doesn't have a field `errors`
        // so ignore auto error detection if the return type has that field
        if (errors?.length) {
          if (!('getFields' in returnType && 'errors' in returnType.getFields())) {
            const aggregatedError = new AggregateError(
              errors.map(normalizeError),
              `${rootTypeName}.${fieldName} failed`
            );
            aggregatedError.stack = null;
            options.logger.debug(`=> Throwing the error ${inspect(aggregatedError)}`);
            return aggregatedError;
          }
        }
        if (responseJson.error) {
          if (!('getFields' in returnType && 'error' in returnType.getFields())) {
            const normalizedError = normalizeError(responseJson.error);
            operationLogger.debug(`=> Throwing the error ${inspect(normalizedError)}`);
            return normalizedError;
          }
        }
        operationLogger.debug(`=> Returning ${inspect(responseJson)}`);
        return responseJson;
      };
      interpolationStrings.push(...Object.values(operationConfig.headers || {}));
      interpolationStrings.push(operationConfig.path);
    }
    const { args: globalArgs } = parseInterpolationStrings(interpolationStrings, operationConfig.argTypeMap);
    rootTypeComposer.addFieldArgs(fieldName, globalArgs);
  }

  options.logger.debug(`Building the executable schema.`);
  return schemaComposer;
}

export function isPubSubOperationConfig(
  operationConfig: JSONSchemaOperationConfig
): operationConfig is JSONSchemaPubSubOperationConfig {
  return 'pubSubTopic' in operationConfig;
}

export function getOperationMetadata(operationConfig: JSONSchemaOperationConfig) {
  let httpMethod: HTTPMethod;
  let operationType: OperationTypeNode;
  let rootTypeName: 'Query' | 'Mutation' | 'Subscription';
  if (isPubSubOperationConfig(operationConfig)) {
    httpMethod = null;
    operationType = 'subscription';
    rootTypeName = 'Subscription';
  } else {
    httpMethod = operationConfig.method;
    // Fix compability with Mesh handler
    operationType = operationConfig.type.toLowerCase() as OperationTypeNode;
    if (!httpMethod) {
      if (operationType === 'mutation') {
        httpMethod = 'POST';
      } else {
        httpMethod = 'GET';
      }
    }
    if (!rootTypeName) {
      if (httpMethod === 'GET') {
        rootTypeName = 'Query';
      }
    }
    rootTypeName = operationType === 'query' ? 'Query' : 'Mutation';
  }
  return {
    httpMethod,
    operationType,
    rootTypeName,
    fieldName: operationConfig.field,
  };
}

export async function buildFinalJSONSchema(options: JSONSchemaLoaderOptions) {
  const finalJsonSchema: JSONSchema = {
    type: 'object',
    title: '_schema',
    properties: {},
    required: ['query'],
  };
  for (const operationConfig of options.operations) {
    const { operationType, rootTypeName, fieldName } = getOperationMetadata(operationConfig);
    const rootTypeDefinition = (finalJsonSchema.properties[operationType] = finalJsonSchema.properties[
      operationType
    ] || {
      type: 'object',
      title: rootTypeName,
      properties: {},
    });
    rootTypeDefinition.properties = rootTypeDefinition.properties || {};
    if (operationConfig.responseSchema) {
      rootTypeDefinition.properties[fieldName] = {
        $ref: operationConfig.responseSchema,
      };
    } else if (operationConfig.responseSample) {
      const sample = await readFileOrUrl(operationConfig.responseSample, {
        cwd: options.cwd,
      }).catch((e: any) => {
        throw new Error(`responseSample - ${e.message}`);
      });
      const generatedSchema = toJsonSchema(sample, {
        required: false,
        objects: {
          additionalProperties: false,
        },
        strings: {
          detectFormat: true,
        },
        arrays: {
          mode: 'first',
        },
      });
      generatedSchema.title = operationConfig.responseTypeName;
      rootTypeDefinition.properties[fieldName] = generatedSchema;
    } else {
      const generatedSchema: JSONSchemaObject = {
        type: 'object',
      };
      generatedSchema.title = operationConfig.responseTypeName;
      rootTypeDefinition.properties[fieldName] = generatedSchema;
    }

    const rootTypeInputPropertyName = operationType + 'Input';
    const rootInputTypeName = rootTypeName + 'Input';
    const rootTypeInputTypeDefinition = (finalJsonSchema.properties[rootTypeInputPropertyName] = finalJsonSchema
      .properties[rootTypeInputPropertyName] || {
      type: 'object',
      title: rootInputTypeName,
      properties: {},
    });
    if (operationConfig.requestSchema) {
      rootTypeInputTypeDefinition.properties[fieldName] = {
        $ref: operationConfig.requestSchema,
      };
    } else if (operationConfig.requestSample) {
      const sample = await readFileOrUrl(operationConfig.requestSample, {
        cwd: options.cwd,
      }).catch((e: any) => {
        throw new Error(`requestSample:${operationConfig.requestSample} cannot be read - ${e.message}`);
      });
      const generatedSchema = toJsonSchema(sample, {
        required: false,
        objects: {
          additionalProperties: false,
        },
        strings: {
          detectFormat: true,
        },
        arrays: {
          mode: 'first',
        },
      });
      generatedSchema.title = operationConfig.requestTypeName;
      rootTypeInputTypeDefinition.properties[fieldName] = generatedSchema;
    }
  }
  options.logger.debug(`Dereferencing JSON Schema to resolve all $refs`);
  const fullyDeferencedSchema = await dereferenceObject(finalJsonSchema, {
    cwd: options.cwd,
  });
  options.logger.debug(`Healing JSON Schema`);
  const healedSchema = await healJSONSchema(fullyDeferencedSchema);
  options.logger.debug(`Building and mapping $refs back to JSON Schema`);
  const fullyReferencedSchema = await referenceJSONSchema(healedSchema as any);
  return fullyReferencedSchema;
}
