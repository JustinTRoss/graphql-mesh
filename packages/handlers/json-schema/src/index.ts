import { StoreProxy } from '@graphql-mesh/store';
import { GetMeshSourceOptions, KeyValueCache, Logger, MeshHandler, MeshPubSub, YamlConfig } from '@graphql-mesh/types';
import { SchemaComposer } from 'graphql-compose';
import { specifiedDirectives } from 'graphql';
import { JsonSchemaWithDiff } from './JsonSchemaWithDiff';
import { dereferenceObject, JSONSchema, JSONSchemaObject } from 'json-machete';
import {
  getComposerFromJSONSchema,
  buildFinalJSONSchema,
  JSONSchemaLoaderOptions,
  JSONSchemaOperationConfig,
  addExecutionLogicToComposer,
} from '@omnigraphql/json-schema';

export default class JsonSchemaHandler implements MeshHandler {
  private config: YamlConfig.JsonSchemaHandler;
  private baseDir: string;
  public cache: KeyValueCache<any>;
  public pubsub: MeshPubSub;
  public jsonSchema: StoreProxy<JSONSchemaObject>;
  private logger: Logger;

  constructor({ config, baseDir, cache, pubsub, store, logger }: GetMeshSourceOptions<YamlConfig.JsonSchemaHandler>) {
    this.config = config;
    this.baseDir = baseDir;
    this.cache = cache;
    this.pubsub = pubsub;
    this.jsonSchema = store.proxy('jsonSchema.json', JsonSchemaWithDiff);
    this.logger = logger;
  }

  async getMeshSource() {
    const options: JSONSchemaLoaderOptions = {
      baseUrl: this.config.baseUrl,
      operationHeaders: this.config.operationHeaders,
      schemaHeaders: this.config.schemaHeaders,
      operations: this.config.operations as unknown as JSONSchemaOperationConfig[],
      disableTimestampScalar: this.config.disableTimestampScalar,
      errorMessage: this.config.errorMessage,
      logger: this.logger,
      cache: this.cache,
      cwd: this.baseDir,
    };
    const finalJSONSchema = await this.jsonSchema.getWithSet(() => buildFinalJSONSchema(options));
    this.logger.debug(`Derefering the bundled JSON Schema`);
    const fullyDeferencedSchema = await dereferenceObject(finalJSONSchema, {
      cwd: this.baseDir,
    });
    this.logger.debug(`Generating GraphQL Schema from the bundled JSON Schema`);
    const visitorResult = await getComposerFromJSONSchema(fullyDeferencedSchema as JSONSchema, this.logger);

    const schemaComposer = visitorResult.output as SchemaComposer;

    // graphql-compose doesn't add @defer and @stream to the schema
    specifiedDirectives.forEach(directive => schemaComposer.addDirective(directive));

    addExecutionLogicToComposer(schemaComposer, options);

    return {
      schema: schemaComposer.buildSchema(),
    };
  }
}
