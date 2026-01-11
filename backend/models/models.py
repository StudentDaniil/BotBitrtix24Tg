from django.db import models
from django.contrib.auth.models import AbstractUser
from django.conf import settings
from cryptography.fernet import Fernet, InvalidToken
import base64
import urllib.parse


def get_fernet():
    """Создаём объект Fernet на основе SECRET_KEY проекта Django."""
    key = base64.urlsafe_b64encode(settings.SECRET_KEY[:32].encode())
    return Fernet(key)


class TelegramUser(AbstractUser):
    """
    Упрощенная модель пользователя Telegram с шифрованным вебхуком Bitrix24
    """
    telegram_id = models.BigIntegerField(unique=True, verbose_name="ID Telegram")

    # Убираем наследование username от AbstractUser и переопределяем его
    username = models.CharField(
        max_length=150,
        unique=False,
        blank=True,
        null=True,
        verbose_name="Username Telegram"
    )

    # Основные поля Telegram
    first_name = models.CharField(max_length=100, blank=True, null=True)
    last_name = models.CharField(max_length=100, blank=True, null=True)
    language_code = models.CharField(max_length=10, blank=True, null=True)
    is_bot = models.BooleanField(default=False)

    # ЕДИНСТВЕННОЕ ПОЛЕ ДЛЯ ВЕБХУКА - зашифрованный полный URL вебхука
    encrypted_webhook_url = models.BinaryField(
        blank=True,
        null=True,
        verbose_name="Зашифрованный полный URL вебхука Bitrix24"
    )

    # Статус подключения (вычисляемое поле)
    is_bitrix_connected = models.BooleanField(default=False)

    # Настройки пользователя
    notifications_enabled = models.BooleanField(default=True)
    timezone = models.CharField(max_length=50, default='Europe/Moscow')
    language = models.CharField(max_length=10, default='ru')
    settings = models.JSONField(default=dict, blank=True)

    # Технические поля
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    email = models.EmailField(blank=True, null=True)

    # Указываем, что используем telegram_id для аутентификации
    USERNAME_FIELD = 'telegram_id'
    REQUIRED_FIELDS = []  # Убираем username из обязательных полей

    class Meta:
        verbose_name = "Пользователь Telegram"
        verbose_name_plural = "Пользователи Telegram"
        ordering = ['-created_at']
        # Указываем явное имя таблицы, чтобы избежать ошибок
        db_table = 'telegram_user'

    def __str__(self):
        return f"{self.telegram_id} - {self.first_name or ''} {self.last_name or ''}"

    # --- Работа с зашифрованным вебхуком ---
    @property
    def webhook_url(self):
        """Возвращает расшифрованный полный URL вебхука"""
        if not self.encrypted_webhook_url:
            return None
        try:
            return get_fernet().decrypt(self.encrypted_webhook_url).decode()
        except InvalidToken:
            return None

    @webhook_url.setter
    def webhook_url(self, value):
        """Шифрует и сохраняет полный URL вебхука"""
        if value:
            self.encrypted_webhook_url = get_fernet().encrypt(value.encode())
        else:
            self.encrypted_webhook_url = None

    # --- Вычисляемые свойства для совместимости ---
    @property
    def portal_url(self):
        """Извлекает portal_url из вебхука (для обратной совместимости)"""
        webhook = self.webhook_url
        if not webhook:
            return None

        try:
            parsed = urllib.parse.urlparse(webhook)
            portal_base = f"{parsed.scheme}://{parsed.netloc}"
            return portal_base
        except:
            return None

    @property
    def access_token(self):
        """Извлекает access_token из вебхука (для обратной совместимости)"""
        webhook = self.webhook_url
        if not webhook:
            return None

        try:
            parsed = urllib.parse.urlparse(webhook)
            path_parts = parsed.path.strip('/').split('/')
            if len(path_parts) >= 3:
                user_number = path_parts[1]
                webhook_token = path_parts[2]
                return f"{user_number}/{webhook_token}"
        except:
            pass
        return None

    # --- Переопределяем save() ---
    def save(self, *args, **kwargs):
        # Автоматически создаем username если он не задан
        if not self.username and self.telegram_id:
            self.username = f"tg_{self.telegram_id}"

        # Определяем статус подключения
        self.is_bitrix_connected = bool(self.webhook_url)

        super().save(*args, **kwargs)

    def disconnect_bitrix(self):
        """Отключение от Bitrix24"""
        self.webhook_url = None
        self.is_bitrix_connected = False
        self.save()