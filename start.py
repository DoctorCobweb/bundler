import boto
import sys
import os
import pprint
from subprocess import call



AWS_BUCKET_KEY   = 'au.com.andretrosky.roll'
RHI_PUB_KEY_NAME = '73487FA275BBE4142E3DCFD53C95E1C17B86447D.asc'
ENCRYPTED_VEC    = 'VEC-spatial-join.gpg'


def getRollFiles(conn):
    print 'in getAwsKeys'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    print 'keys in %s bucket:' % AWS_BUCKET_KEY

    for yar in daBucket.list():
        if yar.name.startswith('results1/') and yar.name.endswith('.csv'):
            pprint.pprint(yar.name)
            #strip out results1/ prefix for filename to save to
            daBucket.get_key(yar.name).get_contents_to_filename(yar.name[9:])
  


def getPubKey(conn):
    print 'in getRhiPubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(RHI_PUB_KEY_NAME).get_contents_to_filename(RHI_PUB_KEY_NAME)
    call(["gpg --import %s", RHI_PUB_KEY_NAME])



def bundleFiles():
    print 'in bundleFiles'
    call(["tar czf VEC-spatial-join.tar.gz *.csv"])



def encryptTarBall():
    print 'encrypting VEC tarball...'
    command1 = "gpg -o %s --encrypt -r 'Rhiannon Butcher <rhiannon@protodata.com.au>' VEC-spatial-join.tar.gz" % ENCRYPTED_VEC

    call([command1])



def seeYaLaterTarball(conn):
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    k = s3.key.Key(daBucket, ENCRYPTED_VEC)
    k.key = ENCRYPTED_VEC

    try:
        k.set_contents_from_filename(ENCRYPTED_VEC)
        print 'SUCCESS: uploaded %s to S3' % ENCRYPTED_VEC 
    except Exception:
        print 'ERROR: could no upload %s to S3' % ENCRYPTED_VEC




if __name__ == '__main__':
    print 'bundler welcomes you'
    print 'connecting to S3...'
    conn = boto.connect_s3()
    print '...connected to S3'

    getRollFiles(conn)
    getPubKey(conn)
    bundleFiles()
    encryptTarBall()
    seeYaLaterTarball(conn)
