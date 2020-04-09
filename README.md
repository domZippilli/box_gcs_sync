# Box GCS Bucket Sync

Synchronize a Box user account's file listings with a GCS bucket.

This utility will walk a Box account's file heirarchy and compare the files with GCS blobs in the bucket. Then it will perform the reverse, listing the bucket's blobs and and comparing them to the Box listing.

Synchronization rules:

- The Box account is considered to be authoritative; files in it will not be deleted or overwritten.
- When a Box file is found that does not exist in GCS, it will be copied to GCS. Same for folders.
- When a GCS blob is found that does not exist in Box, it will be copied to Box. Same for folders.
- When a file in Box is modified, the modified file will be copied to GCS.
- When a file in Box is deleted that was previously synced to GCS, the GCS object will be deleted.
- When a file is found in both Box and GCS with no sync metadata, no action is taken but a warning is generated.

## Setup

### Step 1: Create Box Enterprise Integration

This extension is designed to function as an enterprise integration to Box, using OAuth 2.0 with JWT (Server Authentication). Extensive [documentation](https://developer.box.com/guides/applications/custom-apps/) is available at the Box website.

The application will need OAuth scopes sufficient to impersonate a user and read/write their files.

Once you have configured and authorized the application, use the `Download as JSON` option in the app's configuration screen to get the settings and credentials for this app.

### Step 2: Encrypt the configuration with KMS

Using the gcloud command line, encrypt the Box application configuration with KMS, as it contains secrets. Use the [quickstart](https://cloud.google.com/kms/docs/quickstart) as a reference for how to create a new key and encrypt a plaintext file.

Once this is done, be sure to delete/destroy the plaintext copy of the application configuration.

### Step 3: Install and run the program

This will vary depending on the environment you choose for the runtime. This README assumes some experience with running Python programs.

Using a suitable environment (`pyenv` is superb for desktop use), you can install the requirements for this program with `pip install -r requirements.txt`.

Run the program via the shebang interpreter (`./box_gcs_sync.py`) or with a Python 3 interpreter of your choice (`python3 box_gcs_sync.py`).

The following arguments are required:

```shell
  -c CONFIG, --config CONFIG
                        Box app configuration ciphertext file path (this must
                        be encrypted with a key present in Google Cloud KMS).
  -k KEYNAME, --keyname KEYNAME
                        Fully qualified name of the key to decrypt the
                        configuration ciphertext, as given by `gcloud kms keys
                        list`. Should be in the format projects/[PROJECT]/loca
                        tions/[LOCATION]/keyRings/[KEYRING]/cryptoKeys/[KEY]
  -l LOGIN, --login LOGIN
                        Box user login (user@domain) to impersonate.
  -b BUCKET, --bucket BUCKET
                        GCS bucket to synchronize.
```

An example invocation would be:

`./box_gcs_sync.py -c config/ciphertext.json -k projects/myproject/locations/global/keyRings/myKeyRing/cryptoKeys/myKey -l user@domain.com -b gs://user-box-sync`

## Limitations

The current implementation stores whole files in memory while transferring them. Until this is optional/eliminated, use is best limited to files in the MB range on a machine with plenty of RAM.

## License

Apache 2.0 - See [the LICENSE](/LICENSE) for more information.

## Copyright

Copyright 2020 Google LLC.
