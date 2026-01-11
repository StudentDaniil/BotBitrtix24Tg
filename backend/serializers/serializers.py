from rest_framework import serializers
from models.models import TelegramUser
import urllib.parse


class TelegramUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = TelegramUser
        fields = [
            'id', 'telegram_id', 'username', 'first_name', 'last_name',
            'language_code', 'is_bot', 'is_bitrix_connected',
            'notifications_enabled', 'timezone', 'language', 'settings',
            'created_at', 'updated_at', 'email'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'is_bitrix_connected']


class TelegramUserCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = TelegramUser
        fields = [
            'telegram_id', 'username', 'first_name', 'last_name',
            'language_code', 'is_bot', 'email'
        ]

    def create(self, validated_data):
        # Создаем пользователя с заданными данными
        user = TelegramUser.objects.create_user(
            telegram_id=validated_data['telegram_id'],
            username=validated_data.get('username'),
            first_name=validated_data.get('first_name'),
            last_name=validated_data.get('last_name'),
            language_code=validated_data.get('language_code', 'ru'),
            is_bot=validated_data.get('is_bot', False),
            email=validated_data.get('email')
        )
        return user


class BitrixWebhookSerializer(serializers.Serializer):
    """Сериализатор для полного URL вебхука Bitrix24"""
    full_webhook_url = serializers.CharField(max_length=500)

    def validate_full_webhook_url(self, value):
        """Проверяем, что вебхук имеет правильный формат"""
        value = value.strip().rstrip('/')

        try:
            parsed_url = urllib.parse.urlparse(value)
            path_parts = parsed_url.path.strip('/').split('/')

            if len(path_parts) < 3 or path_parts[0] != 'rest':
                raise serializers.ValidationError(
                    "Некорректный формат вебхука. Ожидается: https://портал.bitrix24.ru/rest/номер/токен/"
                )

            if '.bitrix24.' not in parsed_url.netloc:
                raise serializers.ValidationError(
                    "URL должен содержать bitrix24 в домене"
                )

            return value
        except Exception as e:
            raise serializers.ValidationError(f"Ошибка валидации вебхука: {str(e)}")

    def create_or_update_webhook(self, user):
        """Сохраняет вебхук для пользователя"""
        user.webhook_url = self.validated_data['full_webhook_url']
        user.save()
        return user


class UserSettingsSerializer(serializers.ModelSerializer):
    class Meta:
        model = TelegramUser
        fields = ['notifications_enabled', 'timezone', 'language', 'settings']


class BitrixConnectionTestSerializer(serializers.Serializer):
    """Сериализатор для результата тестирования подключения"""
    success = serializers.BooleanField()
    user_info = serializers.DictField(required=False)
    error = serializers.CharField(required=False)