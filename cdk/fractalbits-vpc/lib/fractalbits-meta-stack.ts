import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { createInstance, createUserData } from './ec2-utils';

interface FractalbitsMetaStackProps extends cdk.StackProps {
  serviceName: string;
  bssUseI3?: boolean;
  availabilityZone?: string;
}

export class FractalbitsMetaStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: FractalbitsMetaStackProps) {
    super(scope, id, props);

    const az = props.availabilityZone ?? this.availabilityZones[this.availabilityZones.length - 1];
    this.vpc = new ec2.Vpc(this, 'FractalbitsMetaStackVpc', {
      vpcName: 'fractalbits-meta-stack-vpc',
      availabilityZones: [az],
      natGateways: 0,
      subnetConfiguration: [
        { name: 'PublicSubnet', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });

    // Add Gateway Endpoint for S3
    this.vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Add Interface Endpoint for EC2 and SSM
    ['SSM', 'SSM_MESSAGES', 'EC2_MESSAGES'].forEach(service => {
      this.vpc.addInterfaceEndpoint(`${service}Endpoint`, {
        service: (ec2.InterfaceVpcEndpointAwsService as any)[service],
        subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      });
    });

    // IAM Role for EC2
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      roleName: 'FractalbitsInstanceRole',
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
      ],
    });

    // Security Group
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'FractalbitsInstanceSG',
      description: 'Allow outbound only for SSM and S3 access',
      allowAllOutbound: true,
    });

    let instance = undefined;
    if (props.serviceName == "nss") {
      const nssInstanceType = ec2.InstanceType.of(ec2.InstanceClass.M7GD, ec2.InstanceSize.XLARGE4);
      instance = createInstance(this, this.vpc, `${props.serviceName}_bench`, ec2.SubnetType.PRIVATE_ISOLATED, nssInstanceType, sg, ec2Role);
      // Create EBS Volume with Multi-Attach capabilities
      const ebsVolume = new ec2.Volume(this, 'MultiAttachVolume', {
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        availabilityZone: az,
        size: cdk.Size.gibibytes(20),
        volumeType: ec2.EbsDeviceVolumeType.IO2,
        iops: 10000,
        enableMultiAttach: true,
      });
      const bucket = new s3.Bucket(this, 'Bucket', {
        // No bucketName provided – name will be auto-generated
        removalPolicy: cdk.RemovalPolicy.DESTROY, // Delete bucket on stack delete
        autoDeleteObjects: true,                  // Empty bucket before deletion
      });

      const nssBootstrapOptions = `nss_server --bucket=${bucket.bucketName} --volume_id=${ebsVolume.volumeId} --meta_stack_testing`;
      instance.addUserData(createUserData(this, nssBootstrapOptions).render());

      // Attach volume
      new ec2.CfnVolumeAttachment(this, 'AttachVolumeToActive', {
        instanceId: instance.instanceId,
        device: '/dev/xvdf',
        volumeId: ebsVolume.volumeId,
      });
    } else {
      const bssInstanceType = props.bssUseI3
          ? ec2.InstanceType.of(ec2.InstanceClass.I3, ec2.InstanceSize.XLARGE2)
          : ec2.InstanceType.of(ec2.InstanceClass.IS4GEN, ec2.InstanceSize.XLARGE);
      instance = createInstance(this, this.vpc, `${props.serviceName}_bench`, ec2.SubnetType.PRIVATE_ISOLATED, bssInstanceType, sg, ec2Role);
      instance.addUserData(createUserData(this, `bss_server --meta_stack_testing`).render());
    }

    new cdk.CfnOutput(this, 'instanceId', {
      value: instance.instanceId,
      description: `EC2 instance ID`,
    });
  }
}
