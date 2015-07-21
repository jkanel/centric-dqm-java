The addons folder contains two files:

> sqljdbc_auth.dll (130kb) is the 64-bit version.
> sqljdbc_auth_x86.dll (114kb) is the 32-bit version.

If running on 64-bit Java, sqljdbc_auth.dll will be used with no changes.

If running on 32-bit Java, perform the following tasks:

1. Rename sqljdbc_auth.dll to sqljdbc_auth_x64.dll
2. Rename sqljdbc_auth_x86.dll to sqljdbc_auth.dll

The application should the run as expected.
