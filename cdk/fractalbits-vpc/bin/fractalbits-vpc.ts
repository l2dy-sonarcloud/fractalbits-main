#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import {FractalbitsVpcStack} from '../lib/fractalbits-vpc-stack';
import {FractalbitsBenchVpcStack} from '../lib/fractalbits-bench-vpc-stack';
import {PeeringStack} from '../lib/fractalbits-peering-stack';
import {FractalbitsMetaStack} from '../lib/fractalbits-meta-stack';
import {FractalbitsHelperStack} from '../lib/fractalbits-helper-stack';

const app = new cdk.App();

const numApiServers = app.node.tryGetContext('numApiServers') ?? 1;
const numBenchClients = app.node.tryGetContext('numBenchClients') ?? 1;
const benchType = app.node.tryGetContext('benchType') ?? null;
const availabilityZone = app.node.tryGetContext('availabilityZone') ?? app.node.tryGetContext('az') ?? undefined;
const bssInstanceTypes = app.node.tryGetContext('bssInstanceTypes') ?? "i8g.xlarge,i8g.2xlarge,i8g.4xlarge";
const browserIp = app.node.tryGetContext('browserIp') ?? null;

const helperStack = new FractalbitsHelperStack(app, 'FractalbitsHelperStack');
const vpcStack = new FractalbitsVpcStack(app, 'FractalbitsVpcStack', {
  env: {},
  browserIp: browserIp,
  numApiServers: numApiServers,
  numBenchClients: numBenchClients,
  benchType: benchType,
  availabilityZone: availabilityZone,
  bssInstanceTypes: bssInstanceTypes,
  deregisterProviderServiceToken: helperStack.deregisterProviderServiceToken,
});
vpcStack.addDependency(helperStack);

if (benchType === "service_endpoint") {
  const benchClientCount = app.node.tryGetContext('benchClientCount') ?? 1;

  const benchVpcStack = new FractalbitsBenchVpcStack(app, 'FractalbitsBenchVpcStack', {
    env: {},
    serviceEndpoint: vpcStack.nlbLoadBalancerDnsName,
    benchClientCount: benchClientCount,
    benchType: benchType,
    deregisterProviderServiceToken: helperStack.deregisterProviderServiceToken,
  });
  benchVpcStack.addDependency(helperStack);

  new PeeringStack(app, 'PeeringStack', {
    vpcA: vpcStack.vpc,
    vpcB: benchVpcStack.vpc,
    env: {},
  });
}

// === meta stack ===
const nssInstanceType = app.node.tryGetContext('nssInstanceType') ?? null;
const nssStack = new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Nss', {
  serviceName: 'nss',
  nssInstanceType: nssInstanceType,
  availabilityZone: availabilityZone,
  deregisterProviderServiceToken: helperStack.deregisterProviderServiceToken,
});
nssStack.addDependency(helperStack);

const bssStack = new FractalbitsMetaStack(app, 'FractalbitsMetaStack-Bss', {
  serviceName: 'bss',
  bssInstanceTypes: bssInstanceTypes,
  availabilityZone: availabilityZone,
  deregisterProviderServiceToken: helperStack.deregisterProviderServiceToken,
});
bssStack.addDependency(helperStack);
