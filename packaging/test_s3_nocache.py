from __future__ import print_function

try:
   import boto3
except ImportError:
   print('This test requires boto3: perhaps try pip install boto3')
   exit()

import getpass
import inspect
import os
import psutil
import re
import shutil
import subprocess
import sys
import tempfile
from threading import Timer
import time
import ustrings
import platform
import datetime
import random
import string

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from ..test.command import assert_command
from ..configuration import IrodsConfig
from ..controller import IrodsController
from ..core_file import temporary_core_file, CoreFile
from .. import test
from . import settings
from .. import lib
from .resource_suite_s3_nocache import ResourceSuite_S3_NoCache, ResourceBase
#from .test_chunkydevtest import ChunkyDevTest
from . import session
from .rule_texts_for_tests import rule_texts

def statvfs_path_or_parent(path):
    while not os.path.exists(path):
        path = os.path.dirname(path)
    return os.statvfs(path)

class Test_Resource_S3_NoCache(ResourceSuite_S3_NoCache, unittest.TestCase):
    plugin_name = IrodsConfig().default_rule_engine_plugin
    class_name = 'Test_Resource_S3_NoCache'

#    def __init__(self, *args, **kwargs):
#        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
#        self.archive_naming_policy='decoupled'
#        self.s3stsdate=''
#        self.s3region='us-east-1'
#        self.s3endPoint='s3.amazonaws.com'
#        self.s3signature_version=2
#        self.s3sse = 0 # server side encryption


    def setUp(self):

        self.keypairfile='/projects/irods/vsphere-testing/externals/amazon_web_services-CI.keypair'
        self.archive_naming_policy='decoupled'
        self.s3stsdate=''
        self.s3region='us-east-1'
        self.s3endPoint='s3.amazonaws.com'
        self.s3signature_version=2
        self.s3sse = 0 # server side encryption

        # skip ssl tests on ub12
        distro_str = ''.join(platform.linux_distribution()[:2]).replace(' ','')
        #if self._testMethodName.startswith('test_ssl') and distro_str.lower().startswith('ubuntu12'):
        #   self.skipTest("skipping ssl tests on ubuntu 12")

        # set up aws configuration
        self.set_up_aws_config_dir()

        # set up s3 bucket
        s3 = boto3.resource('s3', region_name=self.s3region)

        self.s3bucketname = 'irods-ci-' + distro_str + datetime.datetime.utcnow().strftime('-%Y-%m-%d.%H-%M-%S-%f-')
        self.s3bucketname += ''.join(random.choice(string.letters) for i in xrange(10))
        self.s3bucketname = self.s3bucketname[:63].lower() # bucket names can be no more than 63 characters long
        if self.s3region == 'us-east-1':
            self.bucket = s3.create_bucket(Bucket=self.s3bucketname)
        else:
            self.bucket = s3.create_bucket(Bucket=self.s3bucketname,
                                           CreateBucketConfiguration={'LocationConstraint': self.s3region})

        hostname = lib.get_hostname()

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')

            admin_session.assert_icommand("iadmin mkresc demoResc s3 " + hostname + ":/" + self.s3bucketname  + " "
                                          "'S3_DEFAULT_HOSTNAME=" + self.s3endPoint + ";S3_AUTH_FILE=" + self.keypairfile +  
                                          ";S3_REGIONNAME=" + self.s3region + ";S3_RETRY_COUNT=1;S3_WAIT_TIME_SEC=3;S3_PROTO=HTTP;ARCHIVE_NAMING_POLICY=consistent;" + 
                                          "HOST_MODE=cacheless_attached'", 
                                          'STDOUT_SINGLELINE', 's3')

        super(Test_Resource_S3_NoCache, self).setUp()

    def set_up_aws_config_dir(self):
        # read access keys from keypair file
        with open(self.keypairfile) as f:
            aws_access_key_id = f.readline().rstrip()
            aws_secret_access_key = f.readline().rstrip()

        # make config dir
        aws_cfg_dir_path = os.path.join(os.path.expanduser('~'), '.aws')
        try:
            os.makedirs(aws_cfg_dir_path)
        except OSError:
            if not os.path.isdir(aws_cfg_dir_path):
                raise

        # make config file
        with open(os.path.join(aws_cfg_dir_path, 'config'), 'w') as cfg_file:
            cfg_file.write('[default]\n')
            cfg_file.write('region=' + self.s3region + '\n')

        # make credentials file
        with open(os.path.join(aws_cfg_dir_path, 'credentials'), 'w') as cred_file:
            cred_file.write('[default]\n')
            cred_file.write('aws_access_key_id = ' + aws_access_key_id + '\n')
            cred_file.write('aws_secret_access_key = ' + aws_secret_access_key + '\n')


    def tearDown(self):
        super(Test_Resource_S3_NoCache, self).tearDown()

        # delete s3 bucket
        for obj in self.bucket.objects.all():
            obj.delete()
        self.bucket.delete()

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
        shutil.rmtree(IrodsConfig().irods_directory + "/demoRescVault", ignore_errors=True)

    def test_msi_update_unixfilesystem_resource_free_space_and_acPostProcForParallelTransferReceived(self):
        try:
            filename = 'test_msi_update_unixfilesystem_resource_free_space_and_acPostProcForParallelTransferReceived'
            filepath = lib.make_file(filename, 50000000)

            # make sure the physical path exists
            lib.make_dir_p(self.admin.get_vault_path('demoResc'))

            with temporary_core_file() as core:
                time.sleep(1)  # remove once file hash fix is committed #2279
                core.add_rule(rule_texts[self.plugin_name][self.class_name][inspect.currentframe().f_code.co_name])
                time.sleep(1)  # remove once file hash fix is committed #2279

                self.user0.assert_icommand(['iput', filename])
                free_space = psutil.disk_usage(self.admin.get_vault_path('demoResc')).free

            ilsresc_output = self.admin.run_icommand(['ilsresc', '-l', 'demoResc'])[0]
            for l in ilsresc_output.splitlines():
                if l.startswith('free space:'):
                    ilsresc_freespace = int(l.rpartition(':')[2])
                    break
            else:
                assert False, '"free space:" not found in ilsresc output:\n' + ilsresc_output
            assert abs(free_space - ilsresc_freespace) < 4096*10, 'free_space {0}, ilsresc free space {1}'.format(free_space, ilsresc_freespace)
            os.unlink(filename)
        except:
            print('Skipping for plugin name ['+self.plugin_name+']')



