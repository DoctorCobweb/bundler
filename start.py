#bundler simply bundles the spatial joined indv. file into a tarball then encrypts it
#unlike reducer app, all columns from the roll are kept.
#this app is used to aid in validating the spatial joins & target values generated

import boto
import sys
import os
import pprint
from subprocess import call



AWS_BUCKET_KEY     = 'au.com.andretrosky.roll'
RHI_PUB_KEY_NAME   = '73487FA275BBE4142E3DCFD53C95E1C17B86447D.asc'
RHI_UID            = 'Rhiannon Butcher <rhiannon@protodata.com.au>'
DRE_PUB_KEY_NAME   = '0x93FEF9BB.asc'
DRE_UID            = 'andre trosky <andretrosky@gmail.com>'
ROBIN_PUB_KEY_NAME = '95C40075.asc'
ROBIN_UID          = 'Robin de Garis <support@vic.greens.org.au>'
TARBALL_FILENAME   = 'VEC-spatial-join-and-targets.tar.gz'
ENCRYPTED_VEC      = 'VEC-spatial-join-and-targets.gpg'


def getRollFiles(conn):
    print 'in getAwsKeys'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    print 'keys in %s bucket:' % AWS_BUCKET_KEY

    rollFileNames = []

    for yar in daBucket.list():
        if yar.name.startswith('results_full/') and yar.name.endswith('.csv'):
            pprint.pprint(yar.name)
            #strip out results_full/ prefix for filename to save to
            daBucket.get_key(yar.name).get_contents_to_filename(yar.name[13:])
            rollFileNames.append(yar.name[13:])

    assert len(rollFileNames) > 0, 'ASSERT ERROR: rollFileNames is len 0'

    return rollFileNames
  

def getRhiPubKey(conn):
    print 'in getRhiPubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(RHI_PUB_KEY_NAME).get_contents_to_filename(RHI_PUB_KEY_NAME)
 
    assert os.path.exists(RHI_PUB_KEY_NAME) == True, 'ASSERT ERROR: rhi pub key not exist'

    cmd = ["gpg", "--import", RHI_PUB_KEY_NAME]

    try:
        call(cmd)
    except:
        print 'ERROR:getRhiPubKey subprocess error'
        exit(1)


def getDrePubKey(conn):
    print 'in getDrePubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(DRE_PUB_KEY_NAME).get_contents_to_filename(DRE_PUB_KEY_NAME)

    assert os.path.exists(DRE_PUB_KEY_NAME) == True, 'ASSERT ERROR: dre pub key not exist'

    cmd = ["gpg", "--import", DRE_PUB_KEY_NAME]

    try:
        call(cmd)
    except:
        print 'ERROR:getDrePubKey subprocess error'
        exit(1)


def getRobinePubKey(conn):
    print 'in getRobinPubKey'
    # bootstrap used to download
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    daBucket.get_key(ROBIN_PUB_KEY_NAME).get_contents_to_filename(ROBIN_PUB_KEY_NAME)

    assert os.path.exists(ROBIN_PUB_KEY_NAME) == True, 'ASSERT ERROR: robin pub key not exist'

    cmd = ["gpg", "--import", ROBIN_PUB_KEY_NAME]

    try:
        call(cmd)
    except:
        print 'ERROR:getDrePubKey subprocess error'
        exit(1)



def bundleFiles(rollFileNames):
    print 'in bundleFiles'

    cmd = ["tar", "czf", TARBALL_FILENAME]
    for f in rollFileNames:
        cmd.append(f)
    
    try:
        call(cmd)
    except:
        print 'ERROR:problem budling files'
        exit(1)



def encryptTarBall(UID):
    print 'encrypting VEC tarball...'
    cmd = ["gpg", "-o", ENCRYPTED_VEC, "--encrypt", "-r", UID , TARBALL_FILENAME]

    try:
        call(cmd)
    except:
        print 'ERROR:problem encrypting files'
        exit(1)



def seeYaLaterTarball(conn):
    print 'kicking a tarball goal at s3 stadium'
    daBucket = conn.get_bucket(AWS_BUCKET_KEY)
    k = boto.s3.key.Key(daBucket, ENCRYPTED_VEC)
    k.key = ENCRYPTED_VEC
    assert k.key is not None, 'ASSERT ERROR: k.key is None'

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
    #encryptTarBall(RHI_UID)
    encryptTarBall(ROBIN_UID)
    seeYaLaterTarball(conn)
