#!/usr/bin/env python
# encoding: utf-8

"""
@Author : wangzhaoyun
@Contact:1205108909@qq.com
@File : EmailHelper.py
@Time : 2020/7/31 8:33 
"""

import threading
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from configparser import RawConfigParser
import Log


class EmailHelper(object):
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls, *args, **kwargs):
        with EmailHelper._instance_lock:
            if not hasattr(EmailHelper, "_instance"):
                EmailHelper._instance = EmailHelper(*args, **kwargs)
        return EmailHelper._instance

    def __init__(self):
        self.content = ''
        cfg = RawConfigParser()
        cfg.read('config.ini')
        to_receivers = cfg.get('Email', 'to_receiver')
        cc_receivers = cfg.get('Email', 'cc_receiver')
        self.receivers = list(set(to_receivers.split(';') + cc_receivers.split(';')))  # 接收邮箱

        self.sender = cfg.get('Email', 'sender')
        self.pwd = cfg.get('Email', 'pwd')
        self.server = cfg.get('Email', 'server')
        self.log = Log.get_logger(__name__)

    def sendEmail(self, filePath, fileName, start, end):
        # 创建一个带附件的实例
        message = MIMEMultipart()
        # message['From'] = Header("交易效果:", 'utf-8')
        # message['To'] = Header("交易效果:", 'utf-8')
        subject = f'{start}-{end}交易效果'
        message['Subject'] = Header(subject, 'utf-8')

        # 邮件正文内容
        message.attach(MIMEText(f'这是{start}-{end}交易效果,请查收', 'plain', 'utf-8'))

        # 构造附件1，传送当前目录下的 test.txt 文件
        att1 = MIMEText(open(filePath, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = f'attachment; filename="{fileName}"'
        message.attach(att1)

        # # 构造附件2，传送当前目录下的 runoob.txt 文件
        # att2 = MIMEText(open('runoob.txt', 'rb').read(), 'base64', 'utf-8')
        # att2["Content-Type"] = 'application/octet-stream'
        # att2["Content-Disposition"] = 'attachment; filename="runoob.txt"'
        # message.attach(att2)

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(self.server)
            smtpObj.login(self.sender, self.pwd)
            smtpObj.sendmail(self.sender, self.receivers, message.as_string())
            print("邮件发送成功")
        except smtplib.SMTPException as e:
            print(e)
            print("Error: 无法发送邮件")

    def add_email_content(self, text):
        """
        追加邮件文字内容
        :param text:全局追加邮件文字内容
        """
        self.content += text + ';'

    def send_mail_text(self, subject, is_clear_content=True):
        """
        发送只带文字的邮件
        :param subject:邮件主题
        :param is_clear_content: 发送邮件后是否删除邮件内容，连续发邮件时若邮件独立则True，邮件与邮件之间内容是追加关系则False
        """
        if len(self.content) == 0:
            return
        message = MIMEText(self.content, 'plain', 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(self.server)
            smtpObj.login(self.sender, self.pwd)
            smtpObj.sendmail(self.sender, self.receivers, message.as_string())
            print("邮件发送成功")
            if is_clear_content:
                self.content = ''
        except smtplib.SMTPException as e:
            print(e)
            print("Error: 无法发送邮件")

    def send_email_file(self, file_path, file_name, df_receive, is_clear_content=True):
        """
        发送带文件的邮件
        :param file_path:邮件路径
        :param file_name:邮件中文件名称
        :param subject:邮件主题
        :param is_clear_content: 发送邮件后是否删除content，如果邮件独立True，如果邮件与邮件之间是追加关系则False
        """
        if len(self.content) == 0:
            return
        if df_receive.shape[0] == 0:
            return
        to_receiver = df_receive.iloc[0, :]['to_receiver'].split(';')
        cc_receiver = df_receive.iloc[0, :]['cc_receiver'].split(';')
        clientName = df_receive.iloc[0, :]['clientName']
        clientId = df_receive.iloc[0, :]['clientId']
        tradingDay = df_receive.iloc[0, :]['tradingDay']
        subject = f'算法交易报告:{clientName}({clientId})_{tradingDay}'
        # 创建一个带附件的实例
        message = MIMEMultipart()
        message['From'] = self.sender
        message['To'] = ";".join(to_receiver)
        message['Cc'] = ";".join(cc_receiver)
        message['Subject'] = Header(subject, 'utf-8')

        self.receivers = list(set(to_receiver + cc_receiver))

        # 邮件正文内容
        message.attach(MIMEText(self.content, 'plain', 'utf-8'))

        # 构造附件1，传送当前目录下的 test.txt 文件
        att1 = MIMEText(open(file_path, 'rb').read(), 'plain', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = f'attachment; filename="{file_name}"'
        message.attach(att1)
        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(self.server)
            smtpObj.login(self.sender, self.pwd)
            # smtpObj.sendmail(self.sender, self.receivers, message.as_string())
            print("邮件发送成功")
            if is_clear_content:
                self.content == ''
        except smtplib.SMTPException as e:
            print(e)
            print("Error: 无法发送邮件")


if __name__ == '__main__':
    email = EmailHelper()
    email.sendEmail()
