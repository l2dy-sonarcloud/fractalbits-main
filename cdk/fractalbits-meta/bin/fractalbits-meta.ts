#!/home/linuxbrew/.linuxbrew/opt/node/bin/node
import * as cdk from 'aws-cdk-lib';
import { FractalbitsMetaStack } from '../lib/fractalbits-meta-stack';

const app = new cdk.App();
new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Nss', {
  serviceName: 'nss',
});

new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Bss', {
  serviceName: 'bss',
});
