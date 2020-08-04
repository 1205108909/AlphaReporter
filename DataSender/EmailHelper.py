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
from email.mime.base import MIMEBase
from email.header import Header
from email import encoders
from configparser import RawConfigParser
import os
from Constants import Constants


class EmailHelper(object):
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls, *args, **kwargs):
        with EmailHelper._instance_lock:
            if not hasattr(EmailHelper, "_instance"):
                EmailHelper._instance = EmailHelper(*args, **kwargs)
        return EmailHelper._instance

    def __init__(self):
        cfg = RawConfigParser()
        cfg.read('config.ini')
        receivers = cfg.get('Email', 'receiveList')
        self.receivers = receivers.split(';')  # 接收邮箱
        self.sender = cfg.get('Email', 'sender')
        self.pwd = cfg.get('Email', 'pwd')
        self.server = cfg.get('Email', 'server')

    def sendEmail(self, filePath,fileName,start,end):
        # 创建一个带附件的实例
        message = MIMEMultipart()
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


if __name__ == '__main__':
    email = EmailHelper()
    email.sendEmail()
