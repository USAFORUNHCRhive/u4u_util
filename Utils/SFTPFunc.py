"""Uses paramiko to access and operate in remote SFTP servers"""

import io
from datetime import datetime
import paramiko as pk
import re


class AccessFtp:
    def __init__(self, host, userName, password=""):
        self.host = host
        self.userName = userName
        self.password = password

    def loginToFtp(self):
        client = pk.SSHClient()
        client.set_missing_host_key_policy(pk.AutoAddPolicy())
        client.connect(
            hostname=self.host, username=self.userName, password=self.password
        )
        transport = client.get_transport()
        transport.default_window_size = pk.common.MAX_WINDOW_SIZE
        transport.packetizer.REKEY_BYTES = pow(
            2, 40
        )  # 1TB max, this is a security degradation!
        transport.packetizer.REKEY_PACKETS = pow(2, 40)  #
        return client.open_sftp()

    def loginToFtpPort(self, port):
        client = pk.SSHClient()
        client.set_missing_host_key_policy(pk.AutoAddPolicy())
        client.connect(
            hostname=self.host,
            username=self.userName,
            password=self.password,
            port=port,
        )
        return client.open_sftp()

    def getFilesInDirectoryList(self, sftp_client, dirPath):
        sftp_client.chdir("%s/" % dirPath)

        return sftp_client.listdir_attr()

    def loginToFtpWithKeyFile(self, keyfile):
        """
        This method logs into an sftp server with a private keyfile
        @param keyfile: path/name of priavate keyfile
        """
        mykey = pk.RSAKey.from_private_key_file(keyfile)
        client = pk.SSHClient()
        client.set_missing_host_key_policy(pk.AutoAddPolicy())
        client.connect(
            hostname=self.host, username=self.userName, pkey=mykey, look_for_keys=False
        )

        return client.open_sftp()

    def loginToFtpWithCredentialKey(self, string_keyfile):
        client = pk.SSHClient()
        client.set_missing_host_key_policy(pk.AutoAddPolicy())

        f = open(string_keyfile, "r")
        s = f.read()
        if re.match(r"\n", "s"):
            second_replace = s
        else:
            first_replace = re.sub(
                r"-----BEGIN RSA PRIVATE KEY----- ",
                r"-----BEGIN RSA PRIVATE KEY-----\n",
                s,
            )
            second_replace = re.sub(
                r" -----END RSA PRIVATE KEY-----",
                r"\n-----END RSA PRIVATE KEY-----",
                first_replace,
            )
        keyfile = io.StringIO(second_replace)
        mykey = pk.RSAKey.from_private_key(keyfile)
        client.connect(
            hostname=self.host, username=self.userName, pkey=mykey, look_for_keys=False
        )

        return client.open_sftp()

    def getFile(self, sftp_client, pathToRemote, fileName):
        """This method will get a file in the FTP and save it locally. The timeout is how long the connection will be open
        for.

        @param fileName: The name of the file in the ftp
        @return: A saved file in the local directory retrieved from the ftp.
        """

        return sftp_client.get(f"{pathToRemote}/{fileName}", fileName)

    def checkStat(self, sftp_client, pathToRemote):
        return sftp_client.stat(pathToRemote)

    def getHiveOutputZipFile(self, filePrefix, dirPath, date):
        """
        This method will get a find a zip-file in the ftp based on the prefix and date given c

        @param filePrefix: The Prefix of the file in the ftp
        @param date: The date in which the prefix ftp file should be unzipped for.

        @return: A local file Zipped based on the filePrefix and date it was stored in the FTP
        """

        [print(attr.filename) for attr in self.getFilesInDirectoryList(dirPath)]

        lst = [
            [attr.filename, datetime.fromtimestamp(attr.st_mtime).strftime("%Y%m%d")]
            for attr in self.getFilesInDirectoryList(dirPath)
        ]
        listFiles = list(filter(lambda k: filePrefix in k[0] and k[1] == date, lst))

        try:
            fileToZip = listFiles[0][0]
            return fileToZip
        except IndexError:
            print("No Data to Unzip For prefix -- %s on %s" % (filePrefix, date))

    def uploadFileToFtp(self, sftp_client, file_name, data_frame):
        with sftp_client.open("%s" % file_name, "w") as f:
            f.write(
                data_frame.to_csv(
                    index=False, date_format="%m/%d/%y", float_format="%.2f"
                )
            )

    def uploadFileToFtpWithPath(self, sftp_client, directory, file_name, data_frame):
        sftp_client.chdir(directory)
        print(sftp_client.listdir_attr())

        with sftp_client.open("%s" % file_name, "w") as f:
            f.write(
                data_frame.to_csv(
                    index=False, date_format="%m/%d/%y", float_format="%.2f"
                )
            )

    def uploadFileToFtpPort(self, sftp_client, port, directory, file_name, data):
        login = self.loginToFtpPort(port)
        login.chdir(directory)
        print(login.listdir_attr())

        with login.open("%s" % file_name, "w") as f:
            f.write(data)
