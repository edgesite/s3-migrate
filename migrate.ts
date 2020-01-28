import {S3Client} from '@aws-sdk/client-s3-node/S3Client';
import {GetObjectCommand, GetObjectExceptionsUnion} from '@aws-sdk/client-s3-node/commands/GetObjectCommand';
import {PutObjectCommand} from '@aws-sdk/client-s3-node/commands/PutObjectCommand';
import {ListObjectsCommand} from '@aws-sdk/client-s3-node/commands/ListObjectsCommand';
import {Sema} from 'async-sema';

interface Context {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  endpoint: string;
  Bucket: string;
  Prefix: string;
}

function buildClient(ctx: Context) {
  return new S3Client({
    credentials: {
      accessKeyId: ctx.accessKeyId,
      secretAccessKey: ctx.secretAccessKey,
    },
    region: ctx.region,
    endpoint: ctx.endpoint,
  });
}

async function* listDir(client: S3Client, bucket: string, prefix: string, maxLimit = Infinity) {
  let Marker: any;
  for (let count = 0; count < maxLimit;) {
    const result = await client.send(new ListObjectsCommand({
      Prefix: prefix,
      Bucket: bucket,
      Marker,
    }));
    if (!result.Contents || result.Contents.length == 0) break;
    for (let item of result.Contents) {
      yield item;
    }
    Marker = result.Contents[result.Contents.length-1].Key;
  }
}

async function copy(src: {
  client: S3Client;
  key: string;
  bucket: string;
}, dst: {
  client: S3Client;
  key: string;
  bucket: string;
}) {
  const r = await src.client.send(new GetObjectCommand({
    Bucket: src.bucket,
    Key: src.key,
  }));
  await dst.client.send(new PutObjectCommand({
    Bucket: dst.bucket,
    Key: dst.key,
    Body: r.Body,
  }));
}

async function migrate(src: Context, dst: Context, concurrent = 10) {
  const sema = new Sema(concurrent);

  const srcClient = buildClient(src);
  const dstClient = buildClient(dst);

  for await (let item of listDir(srcClient, src.Bucket, src.Prefix)) {
    (async () => {
      await sema.acquire();
      try {
        await copy(
          {client: srcClient, key: item.Key!, bucket: src.Bucket},
          {client: dstClient, key: item.Key!, bucket: dst.Bucket},
        );
        console.log(item.Key, 'completed');
      } finally {
        sema.release();
      }
    })();
  }
}

migrate({
  accessKeyId: '',
  secretAccessKey: '',
  region: 'eu-central',
  endpoint: 'https://s3.eu-central.stackpathstorage.com',
  Bucket: '',
  Prefix: '',
}, {
  accessKeyId: '',
  secretAccessKey: '',
  region: 'eu-central-1',
  endpoint: 'https://s3.eu-central-1.wasabisys.com',
  Bucket: '',
  Prefix: '',
});
