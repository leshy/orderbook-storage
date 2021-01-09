import { inspect } from 'util'
import WebSocket from 'ws'
import Ajv from 'ajv'

import { Observable } from 'rxjs'
import { throttleTime, map, filter } from 'rxjs/operators';

const ajv = new Ajv()

export type BasicType = string | number | boolean | void

export interface DeepDict<Leaf> {
  [key: string]: DeepDict<Leaf> | Leaf
}

export type Json =
  | BasicType
  | Array<BasicType>
  | DeepDict<BasicType | Array<BasicType>>

enum State {
  Init,
  Connected,
  Subscribed,
  Disconnected,
  Error
}

type Logger = {
  log: (data: string) => void
}

type InitClient = {
  state: State.Init
  logger: Logger
  url: string
}

type ConnectedClient = {
  url: string
  logger: Logger
  state: State.Connected
  ws: WebSocket
}

type Topic<T> = Observable<T>

type SubscribedClient = {
  logger: Logger
  state: State.Subscribed
  topics: { [name: string]: Topic<unknown> }
  ws: WebSocket
  url: string
}

// random meaningful state categories for functions
type OpenClient = SubscribedClient | ConnectedClient
type ClosedClient = InitClient

const connect = (initClient: InitClient): Promise<ConnectedClient> => new Promise((resolve) => {
  const ws = new WebSocket(initClient.url)
  ws.on('open', () => resolve({ ...initClient, state: State.Connected, ws }))
})

async function send(client: OpenClient, data: any) {
  return client.ws.send(JSON.stringify(data))
}

async function ensureOpenClient(client: ClosedClient | OpenClient): Promise<OpenClient> {
  if (client.state === State.Init) {
    return connect(client)
  } else {
    return Promise.resolve(client)
  }
}

async function subscribe(client: OpenClient, topicName: string): Promise<[SubscribedClient, Topic<Json>]> {
  const parseJson = (data: any) => {
    try {
      return JSON.parse(data)
    } catch (error) {
      console.error(error, data)
      return {}
    }
  }

  const topic: Topic<Json> = new Observable(subscriber => {
    client.logger.log('subscribing to ' + topicName)

    send(client, {
      "event": "bts:subscribe",
      "data": {
        "channel": topicName
      }
    })

    client.ws.on('message', function incoming(data: string) {
      subscriber.next(data)
    })
  }).pipe(map(parseJson))

  const subscribedClient = {

    ...client,
    state: State.Subscribed,
    topics: { ...((client.state === State.Subscribed) ? client.topics : {}), [topicName]: topic }
  }
  // @ts-ignore
  return [subscribedClient, topic]
}

async function subscribeWithSchema<T extends Object>(client: OpenClient, topicName: string, schema: any): Promise<[SubscribedClient, Topic<T>]> {
  const [subscribedClient, topic] = await subscribe(client, topicName)
  const validate = ajv.compile(schema)
  return [subscribedClient, (topic.pipe(filter((data) => validate(data))) as unknown as Topic<T>)]
}

type OrderBookData = {
  timestamp: number,
  bids: Array<[number, number]>,
  asks: Array<[number, number]>
}

type OrderBook = Topic<OrderBookData>

async function orderBook(client: OpenClient, currency: string): Promise<[SubscribedClient, OrderBook]> {

  const [subscribedClient, topic] = await subscribeWithSchema(client, 'order_book_' + currency, {
    type: 'object',
    properties: {
      data: {
        type: "object",
        properties: {
          timestamp: { "type": 'string', minLength: 10, pattern: "^[0-9]*$" },
          microtimestamp: { "type": 'string', minLength: 16, pattern: "^[0-9]*$" },
          bids: { type: 'array', items: { type: 'array', minItems: 2, maxItems: 2 } },
          asks: { type: 'array', items: { type: 'array', minItems: 2, maxItems: 2 } }
        },
        required: ["bids", "asks", "timestamp", "microtimestamp"]
      }
    },
    required: ["data"]
  })

  type PreTransform = {
    data: {
      timestamp: string
      microtimestamp: string,
      bids: Array<[number, number]>,
      asks: Array<[number, number]>
    }
  }

  const transform = (data: PreTransform): OrderBookData => ({
    timestamp: Number(data.data.timestamp),
    asks: data.data.asks,
    bids: data.data.bids
  })

  return [subscribedClient, (topic as Topic<Json>).pipe(
    // @ts-ignore
    throttleTime(1000),
    map(transform)
  )]
}

const init = async () => {
  const logger = { log: (data: string) => console.log(data) }
  const connectedClient = await connect({ state: State.Init, logger, url: 'wss://ws.bitstamp.net' })
  const [_, orderbook] = await orderBook(connectedClient, 'btceur')
  orderbook.subscribe((value: OrderBookData) => console.log(inspect(value, { depth: 8, colors: true })))
}

init()



