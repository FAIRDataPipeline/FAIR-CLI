### content of "create_user.py" file
from django.contrib.auth import get_user_model

# see ref. below
UserModel = get_user_model()

if not UserModel.objects.filter(username='FAIRDataPipeline').exists():
    user=UserModel.objects.create_user('FAIRDataPipeline', password='Pa55word')
    user.is_superuser=False
    user.is_staff=True
    user.save()