import boto3

from .session import SQSExtendedClientSession


# Monkey patch to use our Session object instead of boto3's
boto3.session.Session = SQSExtendedClientSession

# Now take care of the reference in the boto3.__init__ module
setattr(boto3, 'Session', SQSExtendedClientSession)

# Now ensure that even the default session is our SQSExtendedClientSession
if boto3.DEFAULT_SESSION:
  boto3.setup_default_session()
