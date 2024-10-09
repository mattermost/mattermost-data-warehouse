from moto import mock_aws

from utils.packets.aws import bucket_exists, object_iter


@mock_aws
def test_bucket_exists(s3_boto):
    # GIVEN: 2 buckets exist
    s3_boto.create_bucket(Bucket='test-bucket-1')
    s3_boto.create_bucket(Bucket='test-bucket-2')

    # THEN: expect existing buckets to be found
    assert bucket_exists('test-bucket-1') is True
    assert bucket_exists('test-bucket-2') is True

    # THEN: expect non-existing bucket to be not found
    assert bucket_exists('test-bucket-1-1') is False
    assert bucket_exists('not-exists') is False


@mock_aws
def test_object_iter_with_prefix(s3_boto):
    # GIVEN: a bucket with 3 objects exists
    bucket_name = 'bucket-with-files'
    s3_boto.create_bucket(Bucket=bucket_name)
    s3_boto.put_object(Bucket=bucket_name, Key='prefix/2024/09/05/Mattermost/1.txt', Body=b'1')
    s3_boto.put_object(Bucket=bucket_name, Key='prefix/2024/10/04/Mattermost/1.txt', Body=b'2')
    s3_boto.put_object(Bucket=bucket_name, Key='prefix/2024/10/04/Mattermost/2.txt', Body=b'3')

    # WHEN: iterating over objects with prefix
    objects = list(object_iter(bucket_name, 'prefix/2024/10'))

    # THEN: expect two objects to have been loaded
    assert len(objects) == 2
    assert objects[0][0] == 'prefix/2024/10/04/Mattermost/1.txt'
    assert objects[0][1].read() == b'2'

    assert objects[1][0] == 'prefix/2024/10/04/Mattermost/2.txt'
    assert objects[1][1].read() == b'3'


@mock_aws
def test_object_iter_without_prefix(s3_boto):
    # GIVEN: a bucket with 3 objects exists
    bucket_name = 'bucket-with-files'
    s3_boto.create_bucket(Bucket=bucket_name)
    s3_boto.put_object(Bucket=bucket_name, Key='prefix/2024/09/05/Mattermost/1.txt', Body=b'1')
    s3_boto.put_object(Bucket=bucket_name, Key='prefix/2024/10/04/Mattermost/1.txt', Body=b'2')
    s3_boto.put_object(Bucket=bucket_name, Key='prefix/2024/10/04/Mattermost/2.txt', Body=b'3')

    # WHEN: iterating over objects with prefix
    objects = list(object_iter(bucket_name, ''))

    # THEN: expect two objects to have been loaded
    assert len(objects) == 3
    assert objects[0][0] == 'prefix/2024/09/05/Mattermost/1.txt'
    assert objects[0][1].read() == b'1'

    assert objects[1][0] == 'prefix/2024/10/04/Mattermost/1.txt'
    assert objects[1][1].read() == b'2'

    assert objects[2][0] == 'prefix/2024/10/04/Mattermost/2.txt'
    assert objects[2][1].read() == b'3'
