import boto
import sys
import os
import pprint
from subprocess import call



AWS_BUCKET_KEY   = 'au.com.andretrosky.roll'
RHI_PUB_KEY_NAME = '73487FA275BBE4142E3DCFD53C95E1C17B86447D.asc'
RHI_UID          = 'Rhiannon Butcher <rhiannon@protodata.com.au>'
DRE_PUB_KEY_NAME = '0x93FEF9BB.asc'
DRE_UID          = 'andre trosky <andretrosky@gmail.com>'
ENCRYPTED_VEC    = 'VEC-spatial-join-and-targets.gpg'


def getRollFiles(conn):
    print 'in getAwsKeys'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    print 'keys in %s bucket:' % AWS_BUCKET_KEY

    rollFileNames = []

    for yar in daBucket.list():
        if yar.name.startswith('results1/') and yar.name.endswith('.csv'):
            pprint.pprint(yar.name)
            #strip out results1/ prefix for filename to save to
            daBucket.get_key(yar.name).get_contents_to_filename(yar.name[9:])
            rollFileNames.append(yar.name[9:])

    return rollFileNames
  

def getRhiPubKey(conn):
    print 'in getRhiPubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(RHI_PUB_KEY_NAME).get_contents_to_filename(RHI_PUB_KEY_NAME)
    cmd = ["gpg", "--import", RHI_PUB_KEY_NAME]
    call(cmd)



def getDrePubKey(conn):
    print 'in getDrePubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(DRE_PUB_KEY_NAME).get_contents_to_filename(DRE_PUB_KEY_NAME)
    cmd = ["gpg", "--import", DRE_PUB_KEY_NAME]
    call(cmd)



def bundleFiles(rollFileNames):
    print 'in bundleFiles'

    cmd = ["tar", "czf", "VEC-spatial-join.tar.gz"]
    for f in rollFileNames:
        cmd.append(f)
    
    call(cmd)



def encryptTarBall(UID):
    print 'encrypting VEC tarball...'
    cmd = ["gpg", "-o", ENCRYPTED_VEC, "--encrypt", "-r", UID , "VEC-spatial-join.tar.gz"]

    call(cmd)



def seeYaLaterTarball(conn):
    print 'kicking a tarball goal at s3 stadium'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    k = boto.s3.key.Key(daBucket, ENCRYPTED_VEC)
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

    rollFileNames = getRollFiles(conn)
    getRhiPubKey(conn)
    getDrePubKey(conn)
    bundleFiles(rollFileNames)
    #encryptTarBall(DRE_UID)
    encryptTarBall(RHI_UID)
    seeYaLaterTarball(conn)
