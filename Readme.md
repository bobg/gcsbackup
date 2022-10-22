# Gcsbackup

This is gcsbackup, a command for backing up files to a Google Cloud Storage bucket.

**IMPORTANT.** Google Cloud Storage operations cost money.
Be sure you understand the costs you will incur by using this software.
The author is not responsible for GCS expenses you did not expect.

## Installation

```sh
go install github.com/bobg/gcsbackup@latest
```

## Usage

### Backing up files

```sh
gcsbackup [-creds CREDSFILE] [-throttle RATE] -bucket BUCKET save [-exclude-from EXCLUDEFILE] [-list LISTFILE] DIR1 DIR2 ...
```

This saves files in the given DIR trees to the given BUCKET.

A credentials file is required to authorize `gcsbackup` to write to the bucket.
See [Credentials](#credentials) below.

Use `-throttle RATE` to limit upload speed.
RATE is specified in bytes per second.
The default, 0, means “unlimited.”

Use `-exclude-from EXCLUDEFILE` to exclude certain filenames from being backed up.
The file contains one regular-expression pattern per line.
If any pattern matches any part of a given file,
the file is not backed up.

Use `-list LISTFILE` to specify the output of an earlier `gcsbackup list` run on the same bucket.
This is used to know what files are already backed up without having to query GCS,
which can significantly speed things up and reduce costs.

Empty directories, symbolic links, and zero-length files are not backed up.

### Listing bucket contents

```sh
gcsbackup [-creds CREDSFILE] -bucket BUCKET list
```

Lists information about the objects in the given BUCKET.
Output is in the form of a sequence of JSON objects.
This list can be used as input to `gcsbackup save` and `gcsbackup fs`.

A credentials file is required to authorize `gcsbackup` to read from the bucket.
See [Credentials](#credentials) below.

### Mounting a FUSE filesystem

```sh
gcsbackup [-creds CREDSFILE] -bucket BUCKET fs [-name NAME] [-list LISTFILE] MOUNTPOINT
```

Mounts a FUSE filesystem at MOUNTPOINT,
supplying files and directories from the given BUCKET.

Use `-name NAME` to give the filesystem a different name
(used by your operating system).
The default is the name of the bucket.

Use `-list LISTFILE` to specify the output of an earlier `gcsbackup list` run on the same bucket.
This is used to know what files are present in the bucket without having to query GCS,
which can significantly speed things up and reduce costs.

## Credentials

A credentials file is required to authorize `gcsbackup` to perform its operations in GCS.
The default is `creds.json` in the current directory.
It must contain a JSON “service account” or “client ID” token;
see [Credentials](https://cloud.google.com/docs/authentication#credentials).
The default is `creds.json` in the current directory
and can be overridden with the `-creds` flag.

## Storage format

Each file is stored as its own bucket object.
The name of the object is the SHA256 hash of the file’s contents
(expressed as a hex string with a “sha256-” prefix).
The same file in two different locations will thus only get backed up once.

Each object has metadata attached with the name `paths`.
Its value is a JSON object of the form `{PATH: TIME, ...}`,
where PATH is the path at which the file was encountered during `gcsbackup save`,
and TIME is a Unix timestamp (seconds since 1 Jan 1970)
whose value is the time at which the file was backed up.
If the same file was encountered in multiple locations during `gcsbackup save`,
the JSON object will contain multiple paths.
