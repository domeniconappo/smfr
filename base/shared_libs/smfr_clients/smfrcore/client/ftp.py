import os
import socket

import paramiko

from smfrcore.utils import logger


class FTPEfas:
    user = os.getenv('FTP_USER')
    pwd = os.getenv('FTP_PASS')
    host = os.getenv('FTP_HOST')
    server_folder = os.getenv('FTP_PATH')
    local_folder = os.getenv('DOWNLOAD_FOLDER')
    filename_template = os.getenv('RRA_ONDEMAND_FILENAME')

    def __init__(self):
        self.host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
        if self.host not in self.host_keys:
            raise ValueError('SFTP is not known. Run ssh-keyscan -t rsa {} >> ~/.ssh/known_hosts'.format(self.host))

        self.hostkeytype = self.host_keys[self.host].keys()[0]
        self.hostkey = self.host_keys[self.host][self.hostkeytype]
        self.t = paramiko.Transport((self.host, 22))
        self.t.connect(self.hostkey, self.user, self.pwd, gss_host=socket.getfqdn(self.host))
        self.connection = paramiko.SFTPClient.from_transport(self.t)
        self.remote_path = ''
        self.filename = ''
        if not os.path.exists('./latest'):
            logger.warning('+++ No Info about latest fetched RRA')
            self.latest_fetched_date = ''
        else:
            with open('./latest') as f:
                efas_rra_date = f.read()
                self.latest_fetched_date = efas_rra_date
                logger.info('Latest RRA downloaded: %s', self.latest_fetched_date)

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
        name, _ = os.path.splitext(filename)
        return name[:10] if name[:10].isdigit() else name[-10:]

    def download_rra(self, force=False, dated=None):
        dated = dated or 'latest'  # 'latest' or 'YYYYMMDDHH'
        if not self.filename:
            self.filename = self._get_filename(dated)
        efas_rra_date = self.date_from_filename(self.filename)
        if dated == 'latest' and efas_rra_date <= self.latest_fetched_date:
            logger.warning('Latest EFAS RRA already downloaded...%s', efas_rra_date)
            return False

        self.remote_path = os.path.join(self.server_folder, self.filename)
        localfile_path = os.path.join(self.local_folder, os.path.basename(self.filename))
        if not os.path.exists(localfile_path) or force:
            logger.info('-----> downloading new RRA: %s to %s', self.remote_path, localfile_path)
            self.connection.get(self.remote_path, localfile_path)
            self.update_last_downloaded(efas_rra_date)
            return True
        return False

    def close(self):
        self.t.close()

    def update_last_downloaded(self, efas_rra_date):
        with open('./latest', 'w') as f:
            f.write(efas_rra_date)
            self.latest_fetched_date = efas_rra_date


class SFTPClient:
    proxy_cmd = None
    proxy_host = None
    proxy_port = None
    proxy = os.getenv('http_proxy', None)
    if proxy:
        #  e.g. ProxyCommand /usr/local/bin/corkscrew 10.168.209.72 8012 %h %p
        proxy = proxy.lstrip('http://').rstrip('/')
        proxy_host, proxy_port = proxy.split(':')
        proxy_cmd_str = 'corkscrew {} {} {} {}'

    def __init__(self, host, user, passwd, folder=None):
        self.remote_path = '/home/{}'.format(user) if not folder else folder
        self.host = host
        self.user = user
        self.password = passwd
        if self.proxy_host:
            self.proxy_cmd = paramiko.proxy.ProxyCommand(self.proxy_cmd_str.format(
                self.proxy_host, self.proxy_port, host, 22
            ))

        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(host, 22, self.user, self.password, sock=self.proxy_cmd)

        # Using the SSH client, create a SFTP client.
        sftp = self._ssh.open_sftp()
        # Keep a reference to the SSH client in the SFTP client as to prevent the former from
        # being garbage collected and the connection from being closed.
        sftp.sshclient = self._ssh
        self.connection = sftp

    def send(self, path_to_file):
        remote_path_file = os.path.join(self.remote_path, os.path.basename(path_to_file))
        self.connection.put(path_to_file, remote_path_file)

    def close(self):
        self.connection.close()
        self._ssh.close()
