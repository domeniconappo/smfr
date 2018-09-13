import os
import socket

import paramiko


class FTPEfas:
    host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    user = os.environ.get('FTP_USER')
    pwd = os.environ.get('FTP_PASS')
    host = os.environ.get('FTP_HOST')
    server_folder = os.environ.get('FTP_PATH')
    local_folder = os.environ.get('DOWNLOAD_FOLDER')
    filename_template = os.environ.get('RRA_ONDEMAND_FILENAME')

    def __init__(self):

        if self.host not in self.host_keys:
            raise ValueError('SFTP is not known. Run ssh-keyscan -t rsa {} >> ~/.ssh/known_hosts'.format(self.host))

        self.hostkeytype = self.host_keys[self.host].keys()[0]
        self.hostkey = self.host_keys[self.host][self.hostkeytype]
        self.t = paramiko.Transport((self.host, 22))
        self.t.connect(self.hostkey, self.user, self.pwd, gss_host=socket.getfqdn(self.host))
        self.connection = paramiko.SFTPClient.from_transport(self.t)
        self.remote_path = ''
        self.filename = ''

    @property
    def localfilepath(self):
        return os.path.join(self.local_folder, self.filename)

    def rra_date(self, dated):
        if not self.filename:
            self.filename = self._get_filename(dated)
        return self.date_from_filename(self.filename)

    def _get_filename(self, dated):
        if dated == 'latest':
            remote_filelist = self.connection.listdir(self.server_folder)
            res = [f for f in remote_filelist if self._is_rra_file(f)]
            filename = sorted(res, key=self.date_from_filename, reverse=True)[0]
        else:
            filename = self.filename_template.format(dated)
        return filename

    def _is_rra_file(self, f):
        name, _ = os.path.splitext(f)
        template_name, _ = os.path.splitext(self.filename_template)
        template_name = template_name.replace('{}', '')
        return template_name in name

    @classmethod
    def date_from_filename(cls, filename):
        # filename is CostPopEstYYYYMMDDHH.json or stats_fullYYYYMMDDHH.csv or YYYYMMDDHHSM_meanLT_tmp.json
        name, _ = os.path.splitext(filename)
        return name[:10] if name[:10].isdigit() else name[-10:]

    def _fetch(self, force=False, dated=None):
        """

        :param force:
        :return:
        """
        dated = dated or 'latest'  # 'latest' or 'YYYYMMDDHH'
        if not self.filename:
            self.filename = self._get_filename(dated)
        self.remote_path = os.path.join(self.server_folder, self.filename)
        localfile_path = os.path.join(self.local_folder, os.path.basename(self.filename))
        if not os.path.exists(localfile_path) or force:
            self.connection.get(self.remote_path, localfile_path)

    def download_rra(self, force=False, dated=None):
        self._fetch(force, dated)

    def close(self):
        self.t.close()
