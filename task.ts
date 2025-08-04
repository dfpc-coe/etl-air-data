import { Static, Type, TSchema } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, fetch, InputFeatureCollection, InputFeature, DataFlowType, InvocationType } from '@tak-ps/etl';

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
    lastStarted: Type.Optional(Type.Integer()),
    lastStopped: Type.Union([Type.Integer(), Type.Boolean()]),
    latitude: Type.Number(),
    longitude: Type.Number()
});

export default class Task extends ETL {
    static name = 'etl-air-data';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const env = await this.env(InputSchema);

        const features: Static<typeof InputFeature>[] = [];

        const res = await fetch('https://api.airdata.com/broadcasts/recent', {
            headers: {
                Authorization: `Basic ${Buffer.from(env['API Token'] + ':').toString('base64')}`
            }
        });

        const now = +new Date();
        const streams = await res.typed(Type.Array(OutputSchema));

        for (const stream of streams) {
            // Not sure what to use as a persistant ID so using the sid URL param for now
            const share = new URL(stream.shareLink);

            const callsign = `UAS: ${stream.pilotFullName}`


            if (
                typeof stream.lastStopped === 'number'
                && stream.lastStopped > stream.lastStarted
                && stream.lastStopped * 1000 < (now - 600000) // now - 10min (ms)
            ) continue;

            features.push({
                id: `airdata-${share.searchParams.get('sid')}`,
                type: 'Feature',
                properties: {
                    type: 'a-f-A-M',
                    callsign,
                    metadata: stream,
                    video: {
                        uid: `airdata-${share.searchParams.get('sid')}-video`,
                        url: stream.rtmpURL,
                        sensor: `airdata-${share.searchParams.get('sid')}-camera`,
                        connection: {
                            uid: `airdata-${share.searchParams.get('sid')}-video`,
                            address: stream.rtmpURL,
                            networkTimeout: 12000,
                            path: '',
                            protocol: "raw",
                            bufferTime: -1,
                            port: -1,
                            roverPort: -1,
                            rtspReliable: 0,
                            ignoreEmbeddedKLV: false,
                            alias: callsign
                        }
                    }
                },
                geometry: {
                    type: 'Point',
                    coordinates: [ stream.longitude, stream.latitude ]
                }
            })
        }

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}

