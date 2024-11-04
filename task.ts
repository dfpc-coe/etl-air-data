import { Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature } from 'geojson';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, env } from '@tak-ps/etl';

// eslint-disable-next-line @typescript-eslint/no-unused-vars --  Fetch with an additional Response.typed(TypeBox Object) definition
import { fetch } from '@tak-ps/etl';

const InputSchema = Type.Object({
    'API Token': Type.String({
        description: 'Air Data API Token'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({})

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return InputSchema;
        } else {
            return OutputSchema;
        }
    }

    async control(): Promise<void> {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars -- Get the Environment from the Server and ensure it conforms to the schema
        const env = await this.env(InputSchema);

        const features: Feature[] = [];

        // Get things here and convert them to GeoJSON Feature Collections
        // That conform to the node-cot Feature properties spec
        // https://github.com/dfpc-coe/node-CoT/

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}

