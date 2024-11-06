import { Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature } from 'geojson';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, env, fetch } from '@tak-ps/etl';

const InputSchema = Type.Object({
    'API Token': Type.String({
        description: 'Air Data API Token'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const OutputSchema = Type.Object({
    isLive: Type.Integer(),
    personalProfileImageSrc: Type.String(),
    fullAddress: Type.String(),
    pilotFullName: Type.String(),
    shareLink: Type.String(),
    shareLinkPreviewImg: Type.String(),
    rtmpURL: Type.String(),
    lastStarted: Type.Integer(),
    lastStopped: Type.Integer(),
    latitude: Type.Number(),
    longitude: Type.Number()
});

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

        const res = await fetch('https://api.airdata.com/broadcasts/recent', {
            headers: {
                Authorization: `Basic ${Buffer.from(env['API Token'] + ':').toString('base64')}`
            }
        });

        const streams = await res.typed(Type.Array(OutputSchema));

        for (const stream of streams) {
            // Not sure what to use as a persistant ID so using the sid URL param for now
            const share = new URL(stream.shareLink);

            features.push({
                id: `airdata-${share.searchParams.get('sid')}`,
                type: 'Feature',
                properties: {
                    callsign: `UAS: ${stream.pilotFullName}`,
                    metadata: stream
                },
                geometry: {
                    type: 'Point',
                    coordinates: [ stream.longitude, stream.latitude ]
                }
            })
        }

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

