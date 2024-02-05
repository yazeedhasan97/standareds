import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import requests

import utils


class EmailConfig(utils.Model):
    def __init__(self, username, password, server='smtp.gmail.com', port=587, default_sender=None):
        self.__server = server
        self.__port = port
        self.__username = username
        self.__password = password
        self.__default_sender = default_sender

    @property
    def server(self):
        return self.__server

    @server.setter
    def server(self, server):
        if server is None:
            raise ValueError("Server can not be empty or None.")
        if not isinstance(server, str):
            raise ValueError("Server must be a string.")
        self.__server = server

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, port):
        if port is None:
            raise ValueError("Port can not be empty or None.")
        if not isinstance(port, int) or not (0 < port < 65536):
            raise ValueError("Port must be an integer between 1 and 65535.")
        self.__port = port

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, username):
        if username is None:
            raise ValueError("Username can not be empty or None.")
        if not isinstance(username, str):
            raise ValueError("Username must be a string.")
        self.__username = username

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, password):
        if password is not None and not isinstance(password, str):
            raise ValueError("Password must be a string or None.")
        self.__password = password

    @property
    def default_sender(self):
        return self.__default_sender

    @default_sender.setter
    def default_sender(self, default_sender):
        if default_sender is not None and not isinstance(default_sender, str):
            raise ValueError("Default sender must be a string or None.")
        self.__default_sender = default_sender

    # Update method for dynamic configuration
    def update_config(self, **kwargs):
        for key, value in kwargs.items():
            setter_method = f"{key}"
            if hasattr(self, setter_method):
                try:
                    getattr(self, setter_method)(value)
                except Exception as e:
                    raise ValueError(f"Error setting {key}: {str(e)}")
            else:
                raise KeyError(f"Invalid configuration key: {key}")


class MultiPurposeEmailSender:
    def __init__(self, config: EmailConfig, logger):
        self.__config = config
        self._logger = logger

    def send_email(self, subject, body, receivers, attachments=None, inline_attachments=None):
        msg = self._create_message(subject, body, receivers, attachments, inline_attachments)
        try:
            self._logger.info(f"Starting the SMTP server {self.__config.server}:{self.__config.port}.")
            with smtplib.SMTP(self.__config.server, self.__config.port) as server:
                server.starttls()
                self._logger.info(f"Logging into the email {self.__config.username}.")
                server.login(self.__config.username, self.__config.password)
                self._logger.info(f"Sending the email to {receivers}.")
                server.sendmail(self.__config.username, receivers, msg.as_string())
            self._logger.info("Email sent successfully!")
        except smtplib.SMTPException as e:
            self._logger.error(f"Error sending email due server issue: {e}")
        except Exception as e:
            self._logger.error(f"Error sending email due unknown issue: {e}")

    def _create_message(self, subject, body, receivers, attachments=None, inline_attachments=None):
        self._logger.info(f"Start creating the email with subject: {subject}.")
        msg = MIMEMultipart()
        msg['From'] = self.__config.username
        msg['To'] = ", ".join(receivers)
        msg['Subject'] = subject

        # Attach body as HTML
        msg.attach(MIMEText(body, 'html'))

        # Attach files
        self._attach_files(msg, attachments)

        # Attach inline files
        self._attach_files(msg, inline_attachments, inline=True)

        self._logger.info(f"Email with subject: {subject}... created successfully.")
        return msg

    def _attach_files(self, msg, attachments, inline=False):
        if attachments:
            self._logger.info("Adding the attachments to the email.")
            for attachment in attachments:
                try:
                    with open(attachment, "rb") as file:
                        data = file.read()
                    part = MIMEApplication(data, Name=attachment)
                    if not inline:
                        part['Content-Disposition'] = f'attachment; filename="{attachment}"'
                    else:
                        part.add_header('Content-Disposition', 'inline', filename=attachment)
                    msg.attach(part)
                    self._logger.info(f"Attachment {attachment} was added successfully.")
                except Exception as e:
                    self._logger.error(f"Error attaching file '{attachment}': {e}")
            self._logger.info("All attachments were added successfully to the email.")
        else:
            self._logger.info("Attempt to add attachment without providing any.")


class WCConfig:
    def __init__(self, url, token, proxies=None):
        self.__url = url
        self.__token = token
        self.__proxies = proxies

    @property
    def token(self):
        return self.__token

    @token.setter
    def token(self, token):
        if token is None:
            raise ValueError("URL can not be empty or None.")
        if not isinstance(token, str):
            raise ValueError("URL must be a string.")
        self.__token = token

    @property
    def url(self):
        return self.__url

    @url.setter
    def url(self, url):
        if url is None:
            raise ValueError("URL can not be empty or None.")
        if not isinstance(url, str):
            raise ValueError("URL must be a string.")
        self.__url = url

    @property
    def proxies(self):
        if self.__proxies:
            return self.__proxies.copy()
        return {}

    @proxies.setter
    def proxies(self, proxies):
        # You can customize the validation for proxies as needed
        if proxies is not None and not isinstance(proxies, dict):
            raise ValueError("Proxies must be a dictionary or None.")
        self.__proxies = proxies.copy()

    # Update method for dynamic configuration
    def update_config(self, **kwargs):
        for key, value in kwargs.items():
            setter_method = f"set_{key}"
            if hasattr(self, setter_method):
                try:
                    getattr(self, setter_method)(value)
                except Exception as e:
                    raise ValueError(f"Error setting {key}: {str(e)}")
            else:
                raise KeyError(f"Invalid configuration key: {key}")

    # Setters for dynamic configuration updates
    def set_url(self, url):
        self.__url = url

    def set_token(self, url):
        self.__token = url

    def set_proxies(self, proxies):
        self.__proxies = proxies.copy()


class WCSender:
    def __init__(self, config: WCConfig, logger):
        self.__config = config
        self._logger = logger

    def send_message(self, thread_keys: str, message: str, files: list = None, headers: dict = None):
        if headers is None:
            headers = {'Content-type': 'application/json'}
        try:
            message_body = {"recipient": {"thread_key": thread_keys}, "message": {"text": message}}

            # Add file attachments if provided
            if files:
                message_body['message']['attachment'] = self._attach_files(files)

            r = requests.post(
                url=f"{self.__config.url}{self.__config.token}",
                data=json.dumps(message_body),
                headers=headers,
                proxies=self.__config.proxies
            )

            r.raise_for_status()  # Raise HTTPError for bad responses

            # You can return or handle the response as needed
            return r
        except requests.HTTPError as e:
            self._logger.error(f"HTTP error during the WC request: {e.response.text}", )
        except requests.RequestException as e:
            self._logger.error(f"A network error occurred while sending the WC message: {e}")
        except ValueError as e:
            self._logger.error(f"ValueError during the execution of send_message: {e}", )
        except Exception as e:
            self._logger.error(f"An unexpected error occurred during the execution of send_message:{e}", )

    def _attach_files(self, files):
        # Handle file attachments if needed (customize as per your requirements)
        # This method can be extended to support different types of attachments
        attachments = []
        self._logger.info("Adding the attachments to the email.")
        for file_path in files:

            try:
                with open(file_path, 'rb') as file:
                    file_data = file.read()
                file_name = file_path.split('/')[-1]  # Extracting file name from path
                attachments.append({"type": "file", "name": file_name, "content": file_data})
                self._logger.info(f"Attachment {file_path} was added successfully.")
            except FileNotFoundError:
                self._logger.error(f"Attachment file not found: {file_path}", )
            except Exception as e:
                self._logger.error(f"Error processing file {file_path}: {e}")
        self._logger.info("All attachments were added successfully to the message.")
        return attachments
