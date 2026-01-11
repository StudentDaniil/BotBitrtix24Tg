from rest_framework import viewsets, status, permissions
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import get_object_or_404
import aiohttp
import asyncio

from models.models import TelegramUser
from serializers.serializers import (
    TelegramUserSerializer,
    TelegramUserCreateSerializer,
    BitrixWebhookSerializer,
    UserSettingsSerializer,
    BitrixConnectionTestSerializer
)


class TelegramUserViewSet(viewsets.ModelViewSet):
    queryset = TelegramUser.objects.all()
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]

    def get_serializer_class(self):
        if self.action == 'create':
            return TelegramUserCreateSerializer
        return TelegramUserSerializer

    def get_queryset(self):
        if self.request.user.is_superuser:
            return TelegramUser.objects.all()
        elif self.request.user.is_authenticated:
            return TelegramUser.objects.filter(id=self.request.user.id)
        return TelegramUser.objects.none()

    @action(detail=False, methods=['get'])
    def me(self, request):
        serializer = self.get_serializer(request.user)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def by_telegram_id(self, request, pk=None):
        user = get_object_or_404(TelegramUser, telegram_id=pk)
        serializer = self.get_serializer(user)
        return Response(serializer.data)

    @action(detail=True, methods=['post'])
    def connect_bitrix(self, request, pk=None):
        user = self.get_object()

        if not request.user.is_superuser and request.user != user:
            return Response(
                {'detail': 'У вас нет прав для этого действия'},
                status=status.HTTP_403_FORBIDDEN
            )

        serializer = BitrixWebhookSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.create_or_update_webhook(user)
        return Response({
            'status': 'success',
            'message': 'Вебхук успешно подключен',
            'is_connected': user.is_bitrix_connected
        })

    @action(detail=True, methods=['post'])
    def disconnect_bitrix(self, request, pk=None):
        user = self.get_object()

        if not request.user.is_superuser and request.user != user:
            return Response(
                {'detail': 'У вас нет прав для этого действия'},
                status=status.HTTP_403_FORBIDDEN
            )

        user.disconnect_bitrix()
        return Response({
            'status': 'success',
            'message': 'Подключение к Bitrix24 отключено'
        })

    @action(detail=True, methods=['get'])
    def test_bitrix_connection(self, request, pk=None):
        user = self.get_object()

        if not user.is_bitrix_connected:
            return Response({
                'success': False,
                'error': 'Пользователь не подключен к Bitrix24'
            })

        async def test_connection():
            try:
                async with aiohttp.ClientSession() as session:
                    # Используем полный URL вебхука
                    webhook_url = user.webhook_url
                    if not webhook_url:
                        return {
                            'success': False,
                            'error': 'Вебхук не найден'
                        }

                    # Тестируем подключение через метод user.current
                    url = f"{webhook_url.rstrip('/')}/user.current"

                    async with session.post(
                        url,
                        json={},  # Для вебхука auth уже в URL
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:

                        if response.status == 200:
                            data = await response.json()
                            if 'result' in data:
                                return {
                                    'success': True,
                                    'user_info': data['result']
                                }
                            elif 'error' in data:
                                return {
                                    'success': False,
                                    'error': data.get('error_description', 'Ошибка Bitrix24 API')
                                }

                        return {
                            'success': False,
                            'error': f'HTTP {response.status}: {await response.text()}'
                        }

            except asyncio.TimeoutError:
                return {
                    'success': False,
                    'error': 'Таймаут подключения к Bitrix24'
                }
            except Exception as e:
                return {
                    'success': False,
                    'error': str(e)
                }

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(test_connection())
        loop.close()

        serializer = BitrixConnectionTestSerializer(result)
        return Response(serializer.data)

    @action(detail=True, methods=['patch'])
    def update_settings(self, request, pk=None):
        user = self.get_object()

        if not request.user.is_superuser and request.user != user:
            return Response(
                {'detail': 'У вас нет прав для этого действия'},
                status=status.HTTP_403_FORBIDDEN
            )

        serializer = UserSettingsSerializer(user, data=request.data, partial=True)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.save()
        return Response({
            'status': 'success',
            'message': 'Настройки обновлены',
            'settings': {
                'notifications_enabled': user.notifications_enabled,
                'timezone': user.timezone,
                'language': user.language,
                'custom_settings': user.settings
            }
        })


class BitrixTokenByTelegramView(APIView):
    permission_classes = [permissions.AllowAny]

    def get(self, request, telegram_id):
        try:
            user = TelegramUser.objects.get(telegram_id=telegram_id)

            if not user.is_bitrix_connected:
                return Response(
                    {'detail': 'Пользователь не подключен к Bitrix24'},
                    status=status.HTTP_404_NOT_FOUND
                )

            return Response({
                'telegram_id': user.telegram_id,
                'full_webhook_url': user.webhook_url,  # Возвращаем полный вебхук
                'is_connected': user.is_bitrix_connected
            })

        except TelegramUser.DoesNotExist:
            return Response(
                {'detail': 'Пользователь не найден'},
                status=status.HTTP_404_NOT_FOUND
            )

    def post(self, request, telegram_id):
        try:
            user = TelegramUser.objects.get(telegram_id=telegram_id)
        except TelegramUser.DoesNotExist:
            # Создаем нового пользователя если не существует
            user_data = {
                'telegram_id': telegram_id,
                'username': f"tg_{telegram_id}",
                'first_name': request.data.get('first_name', ''),
                'last_name': request.data.get('last_name', ''),
                'language_code': request.data.get('language_code', 'ru'),
                'is_bot': False
            }

            serializer = TelegramUserCreateSerializer(data=user_data)
            if not serializer.is_valid():
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

            user = serializer.save()

        # Сохраняем вебхук
        webhook_serializer = BitrixWebhookSerializer(data=request.data)
        if not webhook_serializer.is_valid():
            return Response(webhook_serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        webhook_serializer.create_or_update_webhook(user)

        return Response({
            'status': 'success',
            'message': 'Вебхук успешно сохранен',
            'telegram_id': user.telegram_id,
            'is_connected': user.is_bitrix_connected
        })

    def delete(self, request, telegram_id):
        try:
            user = TelegramUser.objects.get(telegram_id=telegram_id)
            user.disconnect_bitrix()

            return Response({
                'status': 'success',
                'message': 'Подключение к Bitrix24 отключено'
            })

        except TelegramUser.DoesNotExist:
            return Response(
                {'detail': 'Пользователь не найден'},
                status=status.HTTP_404_NOT_FOUND
            )


@api_view(['POST'])
@permission_classes([permissions.AllowAny])
def test_webhook_connection(request):
    """Тестирование вебхука Bitrix24"""
    serializer = BitrixWebhookSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    webhook_url = serializer.validated_data['full_webhook_url']

    async def test_connection():
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{webhook_url.rstrip('/')}/user.current"

                async with session.post(
                    url,
                    json={},  # Для вебхука auth уже в URL
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:

                    if response.status == 200:
                        data = await response.json()
                        if 'result' in data:
                            return {
                                'success': True,
                                'user_info': {
                                    'id': data['result'].get('ID'),
                                    'name': data['result'].get('NAME'),
                                    'last_name': data['result'].get('LAST_NAME'),
                                    'email': data['result'].get('EMAIL')
                                }
                            }

                    return {
                        'success': False,
                        'error': f'HTTP {response.status}: {await response.text()}'
                    }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(test_connection())
    loop.close()

    return Response(result)


@api_view(['GET'])
@permission_classes([permissions.AllowAny])
def get_user_by_telegram_id(request, telegram_id):
    """Получение пользователя по telegram_id"""
    try:
        user = TelegramUser.objects.get(telegram_id=telegram_id)
        serializer = TelegramUserSerializer(user)
        return Response(serializer.data)
    except TelegramUser.DoesNotExist:
        return Response(
            {'detail': 'Пользователь не найден'},
            status=status.HTTP_404_NOT_FOUND
        )


@api_view(['PATCH'])
@permission_classes([permissions.AllowAny])
def update_user_settings_by_telegram(request, telegram_id):
    """Обновление настроек пользователя по telegram_id"""
    try:
        user = TelegramUser.objects.get(telegram_id=telegram_id)
        serializer = UserSettingsSerializer(user, data=request.data, partial=True)

        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.save()
        return Response({
            'status': 'success',
            'message': 'Настройки обновлены',
            'settings': {
                'notifications_enabled': user.notifications_enabled,
                'timezone': user.timezone,
                'language': user.language,
                'custom_settings': user.settings
            }
        })

    except TelegramUser.DoesNotExist:
        return Response(
            {'detail': 'Пользователь не найден'},
            status=status.HTTP_404_NOT_FOUND
        )