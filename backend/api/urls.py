# urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'users', views.TelegramUserViewSet, basename='user')

urlpatterns = [
    path('', include(router.urls)),
    path(
        'bitrix-token/telegram/<int:telegram_id>/',
        views.BitrixTokenByTelegramView.as_view(),
        name='bitrix-token-by-telegram'
    ),
    path(
        'test-webhook/',
        views.test_webhook_connection,
        name='test-webhook-connection'
    ),
    # Добавьте эти эндпоинты
    path(
        'users/telegram/<int:telegram_id>/',
        views.get_user_by_telegram_id,
        name='get-user-by-telegram'
    ),
    path(
        'users/telegram/<int:telegram_id>/settings/',
        views.update_user_settings_by_telegram,
        name='update-user-settings-by-telegram'
    ),
]