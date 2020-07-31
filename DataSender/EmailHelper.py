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

import os


class EmailHelper(object):
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls, *args, **kwargs):
        with EmailHelper._instance_lock:
            if not hasattr(EmailHelper, "_instance"):
                EmailHelper._instance = EmailHelper(*args, **kwargs)
        return EmailHelper._instance

    def __init__(self):
        # Todo:配置文件解析发件人、分解收件人list

        pass

    def sendEmail(self):
        sender = 'algo@hczq.com'
        receivers = ['wangzhaoyun@hcyjs.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

        # 创建一个带附件的实例
        message = MIMEMultipart()
        message['From'] = Header("菜鸟教程", 'utf-8')
        message['To'] = Header("测试", 'utf-8')
        subject = 'Python SMTP 邮件测试'
        message['Subject'] = Header(subject, 'utf-8')

        # 邮件正文内容
        message.attach(MIMEText('这是Python 邮件发送测试……', 'plain', 'utf-8'))

        # 构造附件1，传送当前目录下的 test.txt 文件
        att1 = MIMEText(open(os.path.join('C:\\Users\\hc01\\Desktop', 'stocks.csv'), 'rb').read(),
                        'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = 'attachment; filename="stocks.csv"'
        message.attach(att1)

        # # 构造附件2，传送当前目录下的 runoob.txt 文件
        # att2 = MIMEText(open('runoob.txt', 'rb').read(), 'base64', 'utf-8')
        # att2["Content-Type"] = 'application/octet-stream'
        # att2["Content-Disposition"] = 'attachment; filename="runoob.txt"'
        # message.attach(att2)

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect('smtp.exmail.qq.com')
            smtpObj.login(sender, '63214677@HCZQ')
            smtpObj.sendmail(sender, receivers, message.as_string())
            print("邮件发送成功")
        except smtplib.SMTPException as e:
            print(e)
            print("Error: 无法发送邮件")


if __name__ == '__main__':
    email = EmailHelper()
    email.sendEmail()
