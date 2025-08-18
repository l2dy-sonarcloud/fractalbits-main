#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import {FractalbitsVpcStack} from '../lib/fractalbits-vpc-stack';
import {FractalbitsBenchVpcStack} from '../lib/fractalbits-bench-vpc-stack';
import {PeeringStack} from '../lib/fractalbits-peering-stack';
import {FractalbitsMetaStack} from '../lib/fractalbits-meta-stack';
import {VpcWithPrivateLinkStack} from "../lib/vpc-with-private-link-stack";

const app = new cdk.App();

const numApiServers = app.node.tryGetContext('numApiServers') ?? 1;
const numBenchClients = app.node.tryGetContext('numBenchClients') ?? 1;
const benchType = app.node.tryGetContext('benchType') ?? null;
const bssInstanceTypes = app.node.tryGetContext('bssInstanceTypes') ?? "i8g.xlarge,i8g.2xlarge,i8g.4xlarge";
const browserIp = app.node.tryGetContext('browserIp') ?? null;
const dataBlobStorage = app.node.tryGetContext('dataBlobStorage') ?? "s3ExpressMultiAz";

const azPairContext = app.node.tryGetContext('azPair');
let azPair: [string, string] | undefined;
if (azPairContext) {
  if (typeof azPairContext === 'string') {
    // Parse comma-separated format: "usw2-az1,usw2-az4"
    const parts = azPairContext.split(',').map(s => s.trim());
    if (parts.length === 2) {
      azPair = [parts[0], parts[1]];
    } else {
      throw new Error(`Invalid azPair format: "${azPairContext}". Expected format: "usw2-az1,usw2-az4"`);
    }
  } else {
    throw new Error(`Invalid azPair format: ${JSON.stringify(azPairContext)}. Expected format: "usw2-az1,usw2-az4"`);
  }
}

const vpcStack = new FractalbitsVpcStack(app, 'FractalbitsVpcStack', {
  env: {},
  browserIp: browserIp,
  numApiServers: numApiServers,
  numBenchClients: numBenchClients,
  benchType: benchType,
  azPair: azPair,
  bssInstanceTypes: bssInstanceTypes,
  dataBlobStorage: dataBlobStorage,
});

if (benchType === "service_endpoint") {
  const benchClientCount = app.node.tryGetContext('benchClientCount') ?? 1;

  const benchVpcStack = new FractalbitsBenchVpcStack(app, 'FractalbitsBenchVpcStack', {
    env: {},
    serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
    benchClientCount: benchClientCount,
    benchType: benchType,
  });

  new PeeringStack(app, 'PeeringStack', {
    vpcA: vpcStack.vpc,
    vpcB: benchVpcStack.vpc,
    env: {},
  });
}

// === meta stack ===
const nssInstanceType = app.node.tryGetContext('nssInstanceType') ?? null;
new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Nss', {
  serviceName: 'nss',
  nssInstanceType: nssInstanceType,
});

new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Bss', {
  serviceName: 'bss',
  bssInstanceTypes: bssInstanceTypes,
});

// === VpcWithPrivateLinkStack ===
new VpcWithPrivateLinkStack(app, 'VpcWithPrivateLinkStack', {
  env: {},
});
