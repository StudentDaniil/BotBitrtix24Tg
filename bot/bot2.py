import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
import urllib.parse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder

import aiohttp
from datetime import datetime, timedelta
import re



# ==================== CONFIGURATION ====================


# ==================== STATE MACHINES ====================
class AuthStates(StatesGroup):
    waiting_webhook = State()


class LeadCreationStates(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_source = State()
    waiting_title = State()


class DealCreationStates(StatesGroup):
    waiting_title = State()
    waiting_stage = State()
    waiting_amount = State()
    waiting_contact = State()


class TaskCreationStates(StatesGroup):
    waiting_title = State()
    waiting_description = State()
    waiting_responsible = State()
    waiting_deadline = State()
    waiting_priority = State()


class ContactCreationStates(StatesGroup):
    waiting_first_name = State()
    waiting_last_name = State()
    waiting_phone = State()
    waiting_email = State()


class DealEditStates(StatesGroup):
    waiting_field = State()
    waiting_value = State()


class TaskEditStates(StatesGroup):
    waiting_field = State()
    waiting_value = State()


class LeadEditStates(StatesGroup):
    waiting_field = State()
    waiting_value = State()


class QuickDealStates(StatesGroup):
    waiting_title = State()
    waiting_amount = State()


class CommentStates(StatesGroup):
    waiting_entity_type = State()
    waiting_entity_id = State()
    waiting_comment = State()


class TaskReassignStates(StatesGroup):
    waiting_task_id = State()
    waiting_responsible = State()


class LeadStatusStates(StatesGroup):
    waiting_lead_id = State()
    waiting_status = State()

def get_main_keyboard():
    """Основная клавиатура с базовыми кнопками"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="/help"),
                KeyboardButton(text="/auth"),
                KeyboardButton(text="/status")
            ],
            [
                KeyboardButton(text="/start"),
                KeyboardButton(text="/logout")
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите команду..."
    )
    return keyboard

# ==================== WEBHOOK PARSER ====================
class WebhookParser:
    @staticmethod
    def parse_webhook_url(webhook_url: str) -> Dict[str, str]:
        """
        Парсит вебхук URL и извлекает portal_url и access_token
        Пример: https://b24-r9de8y.bitrix24.ru/rest/10/abcdef123456/
        Возвращает: {
            'full_webhook_url': webhook_url,
            'portal_url': 'https://b24-r9de8y.bitrix24.ru',
            'user_id': '10',
            'webhook_token': 'abcdef123456'
        }
        """
        try:
            webhook_url = webhook_url.strip().rstrip('/')

            parsed_url = urllib.parse.urlparse(webhook_url)
            path_parts = parsed_url.path.strip('/').split('/')

            if len(path_parts) < 3 or path_parts[0] != 'rest':
                raise ValueError("Некорректный формат вебхука. Ожидается: https://портал.bitrix24.ru/rest/номер/токен/")

            portal_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            user_id = path_parts[1] if len(path_parts) > 1 else ''
            webhook_token = path_parts[2] if len(path_parts) > 2 else ''

            return {
                'full_webhook_url': webhook_url,
                'portal_url': portal_url,
                'user_id': user_id,
                'webhook_token': webhook_token
            }

        except Exception as e:
            raise ValueError(f"Ошибка парсинга вебхука: {str(e)}")

    @staticmethod
    def validate_webhook_url(webhook_url: str) -> bool:
        """Проверяет, является ли URL валидным вебхуком Bitrix24"""
        try:
            result = WebhookParser.parse_webhook_url(webhook_url)
            return all([
                result['full_webhook_url'],
                result['portal_url'],
                result['user_id'],
                result['webhook_token'],
                '.bitrix24.' in result['portal_url']
            ])
        except:
            return False


# ==================== BITRIX API CLIENT ====================
class BitrixAPIClient:
    def __init__(self, webhook_url: str, user_id: str = None):
        self.webhook_url = webhook_url.rstrip('/')
        self.user_id = user_id  # ID пользователя из вебхука (например, '10')
        self.session = None
        self._mask_webhook_url()

        # Если user_id не передан, пытаемся извлечь его из вебхука
        if not self.user_id:
            self._extract_user_id_from_webhook()

    def _extract_user_id_from_webhook(self):
        """Извлекает user_id из URL вебхука"""
        try:
            parsed = urllib.parse.urlparse(self.webhook_url)
            path_parts = parsed.path.strip('/').split('/')
            if len(path_parts) >= 2:
                self.user_id = path_parts[1]  # user_id - это второй элемент пути
        except:
            self.user_id = None

    def _mask_webhook_url(self):
        """Маскирует вебхук URL для безопасного логирования"""
        parsed = urllib.parse.urlparse(self.webhook_url)
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) >= 3:
            # Маскируем токен, оставляя только первые 4 символа
            masked_token = f"{path_parts[2][:4]}***" if len(path_parts[2]) > 4 else "***"
            path_parts[2] = masked_token
        masked_path = '/'.join(path_parts)
        self.masked_url = f"{parsed.scheme}://{parsed.netloc}/{masked_path}"

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        if not self.session:
            self.session = aiohttp.ClientSession()

        params = params or {}

        # Логируем запрос без секретного токена
        logging.info(f"📤 Bitrix24 API запрос: {self.masked_url}/{method}")
        if params:
            # Маскируем чувствительные данные в параметрах
            masked_params = self._mask_sensitive_data(params)
            logging.info(f"📤 Параметры: {masked_params}")

        try:
            start_time = datetime.now()
            async with self.session.post(
                    f"{self.webhook_url}/{method}",
                    json=params,
                    timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                data = await response.json()

                # Логируем результат
                if 'error' in data:
                    error_msg = data.get('error_description', data.get('error', 'Unknown error'))
                    logging.error(f"❌ Bitrix24 API ошибка ({duration:.2f}s): {method} - {error_msg}")
                    raise Exception(f"Bitrix24 API error: {error_msg}")
                else:
                    logging.info(f"✅ Bitrix24 API успех ({duration:.2f}s): {method}")

                return data
        except aiohttp.ClientError as e:
            logging.error(f"❌ Сетевая ошибка при запросе {method}: {str(e)}")
            raise Exception(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            logging.error(f"❌ Таймаут при запросе {method}")
            raise Exception("Request timeout")

    def _mask_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Маскирует чувствительные данные в параметрах для логирования"""
        if not data:
            return data

        masked_data = data.copy()
        sensitive_fields = ['auth', 'token', 'password', 'secret', 'key', 'access_token']

        def mask_value(value):
            if isinstance(value, str) and len(value) > 8:
                return f"{value[:4]}***{value[-4:]}"
            elif isinstance(value, (int, float)):
                return value
            else:
                return "***"

        def mask_dict(d):
            masked = {}
            for key, value in d.items():
                if isinstance(value, dict):
                    masked[key] = mask_dict(value)
                elif isinstance(value, list):
                    masked[key] = [mask_dict(item) if isinstance(item, dict) else mask_value(item) for item in value]
                elif any(sensitive in str(key).lower() for sensitive in sensitive_fields):
                    masked[key] = "***"
                elif isinstance(value, str) and len(value) > 20:
                    masked[key] = f"{value[:10]}...{value[-10:]}"
                else:
                    masked[key] = value
            return masked

        return mask_dict(masked_data)

    # ==================== CRUD ОПЕРАЦИИ ====================

    # -------------------- ЧТЕНИЕ ДАННЫХ --------------------
    async def get_current_user(self):
        """Получение информации о текущем пользователе Bitrix24"""
        logging.info("🔍 Получение текущего пользователя Bitrix24")
        return await self._make_request('user.current')

    async def get_deals(self, filter_params: Dict = None):
        """Получение сделок с фильтрацией по пользователю из вебхука"""
        logging.info(f"📊 Получение списка сделок (user_id: {self.user_id})")
        params = {}

        # Всегда фильтруем по user_id из вебхука
        if self.user_id:
            filter_params = filter_params or {}
            filter_params['ASSIGNED_BY_ID'] = self.user_id

        if filter_params:
            params['filter'] = filter_params

        params['select'] = ['ID', 'TITLE', 'STAGE_ID', 'OPPORTUNITY', 'ASSIGNED_BY_ID', 'DATE_CREATE']
        return await self._make_request('crm.deal.list', params)

    async def get_deal(self, deal_id: str):
        """Получение детальной информации о сделке"""
        logging.info(f"📋 Получение сделки ID: {deal_id}")
        return await self._make_request('crm.deal.get', {'id': deal_id})


    async def get_tasks(self, filter_params: Dict = None):
        """Получение задач с фильтрацией по пользователю из вебхука"""
        logging.info(f"📝 Получение списка задач (user_id: {self.user_id})")

        # Для задач используем другой формат запроса
        params = {
            'order': {'ID': 'DESC'},
            'select': ['ID', 'TITLE', 'STATUS', 'DEADLINE', 'PRIORITY', 'RESPONSIBLE_ID', 'CREATED_DATE', 'DESCRIPTION']
        }

        # Фильтруем по пользователю
        if self.user_id:
            try:
                user_id_int = int(self.user_id)
                params['filter'] = {'RESPONSIBLE_ID': user_id_int}
            except ValueError:
                logging.error(f"❌ Некорректный user_id: {self.user_id}")
                params['filter'] = {'RESPONSIBLE_ID': self.user_id}

        # Добавляем дополнительные фильтры если есть
        if filter_params:
            params['filter'] = params.get('filter', {})
            params['filter'].update(filter_params)

        logging.info(f"📝 Параметры запроса задач: {params}")

        try:
            result = await self._make_request('tasks.task.list', params)

            # Проверяем структуру ответа
            if 'result' in result:
                tasks = result['result'].get('tasks', [])
                logging.info(f"✅ Получено задач: {len(tasks)}")

                # Нормализуем структуру для совместимости с остальным кодом
                normalized_tasks = []
                for task in tasks:
                    normalized_task = {
                        'ID': task.get('id'),
                        'TITLE': task.get('title'),
                        'STATUS': task.get('status'),
                        'DEADLINE': task.get('deadline'),
                        'PRIORITY': task.get('priority'),
                        'RESPONSIBLE_ID': task.get('responsibleId'),
                        'CREATED_DATE': task.get('createdDate'),
                        'DESCRIPTION': task.get('description')
                    }
                    normalized_tasks.append(normalized_task)

                # Возвращаем в формате, ожидаемом обработчиком
                return {'tasks': normalized_tasks}
            else:
                return result

        except Exception as e:
            logging.error(f"❌ Ошибка при получении задач: {str(e)}")
            return {'error': str(e), 'tasks': []}

    async def search_companies(self, query: str):
        """Поиск компаний по названию"""
        logging.info(f"🏢 Поиск компаний: {query}")
        params = {
            'filter': {'%TITLE': f'%{query}%'},
            'select': ['ID', 'TITLE', 'ADDRESS', 'PHONE', 'EMAIL']
        }
        return await self._make_request('crm.company.list', params)

    async def get_task(self, task_id: str):
        """Получение детальной информации о задаче"""
        logging.info(f"📋 Получение задачи ID: {task_id}")

        try:
            params = {'taskId': task_id}
            result = await self._make_request('tasks.task.get', params)

            # Обрабатываем структуру ответа для задач
            if 'result' in result and 'task' in result['result']:
                task = result['result']['task']
                # Нормализуем структуру
                normalized_task = {
                    'ID': task.get('id'),
                    'TITLE': task.get('title'),
                    'STATUS': task.get('status'),
                    'DEADLINE': task.get('deadline'),
                    'PRIORITY': task.get('priority'),
                    'RESPONSIBLE_ID': task.get('responsibleId'),
                    'CREATED_BY': task.get('createdBy'),
                    'CREATED_DATE': task.get('createdDate'),
                    'DESCRIPTION': task.get('description'),
                    'CHANGED_DATE': task.get('changedDate')
                }
                return {'result': normalized_task}
            else:
                return result

        except Exception as e:
            logging.error(f"❌ Ошибка получения задачи {task_id}: {e}")
            return {'error': str(e)}

    async def get_leads(self, filter_params: Dict = None):
        """Получение лидов с фильтрацией по пользователю из вебхука"""
        logging.info(f"🎯 Получение списка лидов (user_id: {self.user_id})")
        params = {}

        # Всегда фильтруем по user_id из вебхука
        if self.user_id:
            filter_params = filter_params or {}
            filter_params['ASSIGNED_BY_ID'] = self.user_id

        if filter_params:
            params['filter'] = filter_params

        params['select'] = ['ID', 'TITLE', 'STATUS_ID', 'SOURCE_ID', 'ASSIGNED_BY_ID', 'DATE_CREATE']
        return await self._make_request('crm.lead.list', params)

    async def get_lead(self, lead_id: str):
        """Получение детальной информации о лиде"""
        logging.info(f"📋 Получение лида ID: {lead_id}")
        return await self._make_request('crm.lead.get', {'id': lead_id})

    async def get_deal_stages(self):
        """Получение списка стадий сделок"""
        logging.info("📊 Получение списка стадий сделок")
        return await self._make_request('crm.dealcategory.stage.list')

    async def get_lead_statuses(self):
        """Получение списка статусов лидов"""
        logging.info("📊 Получение списка статусов лидов")
        return await self._make_request('crm.lead.status.list')

    async def get_users(self):
        """Получение списка пользователей Bitrix24"""
        logging.info("👥 Получение списка пользователей Bitrix24")
        return await self._make_request('user.get')

    # -------------------- СОЗДАНИЕ ДАННЫХ --------------------
    async def create_lead(self, fields: Dict[str, Any]):
        """Создание лида с указанием ответственного из вебхука"""
        logging.info(f"➕ Создание лида: {fields.get('TITLE', 'Без названия')}")

        # Автоматически устанавливаем ASSIGNED_BY_ID из вебхука
        if self.user_id and 'ASSIGNED_BY_ID' not in fields:
            fields['ASSIGNED_BY_ID'] = self.user_id

        return await self._make_request('crm.lead.add', {'fields': fields})

    async def create_deal(self, fields: Dict[str, Any]):
        """Создание сделки с указанием ответственного из вебхука"""
        logging.info(f"💼 Создание сделки: {fields.get('TITLE', 'Без названия')}")

        # Автоматически устанавливаем ASSIGNED_BY_ID из вебхука
        if self.user_id and 'ASSIGNED_BY_ID' not in fields:
            fields['ASSIGNED_BY_ID'] = self.user_id

        return await self._make_request('crm.deal.add', {'fields': fields})

    async def create_task(self, fields: Dict[str, Any]):
        """Создание задачи с указанием ответственного из вебхука"""
        logging.info(f"📌 Создание задачи: {fields.get('TITLE', 'Без названия')}")

        # Автоматически устанавливаем RESPONSIBLE_ID из вебхука
        if self.user_id and 'RESPONSIBLE_ID' not in fields:
            fields['RESPONSIBLE_ID'] = self.user_id

        result = await self._make_request('tasks.task.add', {'fields': fields})

        # Извлекаем только ID задачи из ответа
        if 'result' in result and 'task' in result['result']:
            task_id = result['result']['task'].get('id')
            if task_id:
                return {'result': task_id}

        return result

    async def create_contact(self, fields: Dict[str, Any]):
        """Создание контакта"""
        logging.info(f"👤 Создание контакта: {fields.get('NAME', 'Без имени')}")
        return await self._make_request('crm.contact.add', {'fields': fields})

    # -------------------- ИЗМЕНЕНИЕ ДАННЫХ --------------------
    async def update_deal(self, deal_id: str, fields: Dict[str, Any]):
        """Обновление сделки"""
        logging.info(f"✏️ Обновление сделки ID: {deal_id}")
        return await self._make_request('crm.deal.update', {'id': deal_id, 'fields': fields})

    async def update_task(self, task_id: str, fields: Dict[str, Any]):
        """Обновление задачи"""
        logging.info(f"✏️ Обновление задачи ID: {task_id}")
        return await self._make_request('tasks.task.update', {'taskId': task_id, 'fields': fields})

    async def update_lead(self, lead_id: str, fields: Dict[str, Any]):
        """Обновление лида"""
        logging.info(f"✏️ Обновление лида ID: {lead_id}")
        return await self._make_request('crm.lead.update', {'id': lead_id, 'fields': fields})

    async def add_comment(self, entity_type: str, entity_id: str, comment: str):
        """Добавление комментария к сущности (сделке, задаче, лиду)"""
        method_map = {
            'deal': 'crm.deal.comment.add',
            'task': 'tasks.task.comment.add',
            'lead': 'crm.lead.comment.add'
        }
        logging.info(f"💬 Добавление комментария к {entity_type} ID: {entity_id}")
        params = {
            'id': entity_id,
            'fields': {'COMMENT': comment}
        }
        return await self._make_request(method_map[entity_type], params)

    async def reassign_task(self, task_id: str, responsible_id: str):
        """Переназначение задачи другому пользователю"""
        logging.info(f"🔄 Переназначение задачи {task_id} пользователю {responsible_id}")
        fields = {'RESPONSIBLE_ID': responsible_id}
        return await self.update_task(task_id, fields)

    # -------------------- ОТЧЕТНОСТЬ --------------------
    async def get_deal_report(self, period_start: str, period_end: str):
        """Получение отчета по сделкам с фильтрацией по пользователю из вебхука"""
        logging.info(
            f"📈 Получение отчета по сделкам за период: {period_start} - {period_end} (user_id: {self.user_id})")
        params = {
            'filter': {
                '>=DATE_CREATE': period_start,
                '<=DATE_CREATE': period_end
            },
            'select': ['ID', 'TITLE', 'STAGE_ID', 'OPPORTUNITY', 'DATE_CREATE']
        }

        # Добавляем фильтр по пользователю
        if self.user_id:
            params['filter']['ASSIGNED_BY_ID'] = self.user_id

        return await self._make_request('crm.deal.list', params)

    async def get_task_statistics(self):
        """Получение статистики по задачам пользователя из вебхука"""
        logging.info(f"📊 Получение статистики по задачам (user_id: {self.user_id})")

        # Фильтруем задачи по пользователю
        filter_params = {}
        if self.user_id:
            filter_params['RESPONSIBLE_ID'] = self.user_id

        # Получаем задачи с полем status
        tasks = await self.get_tasks(filter_params)

        stats = {
            'total': 0,
            'completed': 0,
            'in_progress': 0,
            'overdue': 0,
            'pending': 0,
            'deferred': 0,
            'awaiting_control': 0,
            'supposedly_completed': 0
        }

        if 'tasks' in tasks:
            for task in tasks['tasks']:
                stats['total'] += 1

                # Берем статус из поля status (строковый, нужно преобразовать в int)
                status_str = task.get('STATUS', '1')
                try:
                    status = int(status_str)
                except (ValueError, TypeError):
                    status = 1  # По умолчанию новая

                # Получаем дату закрытия для определения завершенности
                closed_date = task.get('closedDate')


                # Проверяем просроченность по дедлайну
                deadline = task.get('deadline')
                is_overdue = False
                if deadline:
                    try:
                        # Парсим дату из строки формата "2026-01-13T00:00:00+03:00"
                        deadline_date = datetime.strptime(deadline[:10], '%Y-%m-%d')
                        if deadline_date < datetime.now() and not closed_date:
                            is_overdue = True
                    except Exception as e:
                        logging.warning(f"Ошибка парсинга даты дедлайна {deadline}: {e}")

                # Классификация по статусам Bitrix24:
                # 1 - новая (принята, но не просмотрена)
                # 2 - ожидает выполнения (просмотрена, но не взята в работу)
                # 3 - выполняется (взята в работу)
                # 4 - ждет контроля
                # 5 - завершена
                # 6 - отложена
                # 7 - отклонена

                if status == 5:  # Завершена
                    stats['completed'] += 1
                elif status == 3:  # Выполняется
                    stats['in_progress'] += 1
                    if is_overdue:
                        stats['overdue'] += 1
                elif status == 2:  # Ожидает выполнения
                    if is_overdue:
                        stats['overdue'] += 1
                    else:
                        stats['pending'] += 1
                elif status == 6:  # Отложена
                    stats['deferred'] += 1
                    if is_overdue:
                        stats['overdue'] += 1
                elif status == 4:  # Ждет контроля
                    stats['awaiting_control'] += 1
                    if is_overdue:
                        stats['overdue'] += 1
                elif status == 1:  # Новая (не просмотрена)
                    stats['pending'] += 1
                    if is_overdue:
                        stats['overdue'] += 1
                else:
                    # Остальные статусы считаем ожидающими
                    stats['pending'] += 1
                    if is_overdue:
                        stats['overdue'] += 1

        logging.info(f"📊 Статистика собрана: {stats}")
        return stats

    async def calculate_deals_sum(self, period_start: str, period_end: str):
        """Расчет суммы сделок за период"""
        logging.info(f"💰 Расчет суммы сделок за период: {period_start} - {period_end} (user_id: {self.user_id})")
        deals = await self.get_deal_report(period_start, period_end)
        total = 0

        for deal in deals.get('result', []):
            amount = deal.get('OPPORTUNITY') or 0
            if isinstance(amount, (int, float)):
                total += amount

        return total

    async def attach_file(self, entity_type: str, entity_id: str, file_data: bytes, filename: str):
        """Прикрепление файла к сущности"""
        method_map = {
            'deal': 'crm.deal.files.attach',
            'task': 'tasks.task.files.attach',
            'lead': 'crm.lead.files.attach'
        }
        logging.info(f"📎 Прикрепление файла к {entity_type} ID: {entity_id}, файл: {filename}")

        import base64
        file_base64 = base64.b64encode(file_data).decode('utf-8')

        params = {
            'id': entity_id,
            'fields': {
                'FILE_DATA': [{
                    'name': filename,
                    'content': file_base64
                }]
            }
        }
        return await self._make_request(method_map[entity_type], params)


# ==================== BACKEND API CLIENT ====================
class BackendAPIClient:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.base_url = "http://localhost:8000/api"
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, method: str, endpoint: str, **kwargs):
        if not self.session:
            self.session = aiohttp.ClientSession()

        base = self.base_url.rstrip('/')
        endpoint_clean = endpoint.lstrip('/')
        url = f"{base}/{endpoint_clean}"

        # Логируем запрос к бэкенду
        logging.info(f"📡 Backend API запрос: {method} {endpoint_clean}")

        headers = kwargs.get('headers', {})
        headers['Content-Type'] = 'application/json'
        kwargs['headers'] = headers

        try:
            start_time = datetime.now()
            async with self.session.request(method, url, **kwargs) as response:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                if response.status in [200, 201]:
                    result = await response.json()
                    logging.info(f"✅ Backend API успех ({duration:.2f}s): {method} {endpoint_clean}")
                    return result
                elif response.status == 204:
                    logging.info(f"✅ Backend API успех (204) ({duration:.2f}s): {method} {endpoint_clean}")
                    return {}
                else:
                    try:
                        error_data = await response.json()
                        error_msg = error_data.get('detail', str(error_data))
                    except:
                        error_msg = await response.text()

                    logging.error(f"❌ Backend API ошибка ({duration:.2f}s): {method} {endpoint_clean} - {error_msg}")
                    raise Exception(f"API error {response.status}: {error_msg}")

        except Exception as e:
            logging.error(f"❌ Ошибка при запросе к бэкенду {method} {endpoint_clean}: {str(e)}")
            raise

    async def get_bitrix_webhook(self) -> Optional[Dict[str, str]]:
        """Получение вебхука Bitrix24 по telegram_id"""
        logging.info(f"🔍 Получение вебхука для пользователя {self.user_id}")
        try:
            data = await self._make_request(
                'GET',
                f'bitrix-token/telegram/{self.user_id}/'
            )
            # Возвращаем полный URL вебхука и его компоненты
            if data and data.get('full_webhook_url'):
                # Парсим вебхук чтобы получить user_id
                webhook_data = WebhookParser.parse_webhook_url(data['full_webhook_url'])
                return {
                    'full_webhook_url': data['full_webhook_url'],
                    'user_id': webhook_data.get('user_id'),
                    'portal_url': webhook_data.get('portal_url'),
                    'webhook_token': webhook_data.get('webhook_token')
                }
            return None
        except Exception as e:
            logging.error(f"❌ Ошибка получения вебхука: {e}")
            return None

    async def save_bitrix_webhook(self, webhook_url: str):
        """Сохранение вебхука Bitrix24"""
        logging.info(f"💾 Сохранение вебхука для пользователя {self.user_id}")
        try:
            # Маскируем вебхук для логирования
            webhook_data = WebhookParser.parse_webhook_url(webhook_url)
            masked_token = f"{webhook_data['webhook_token'][:4]}***" if webhook_data['webhook_token'] else "***"
            masked_url = f"{webhook_data['portal_url']}/rest/{webhook_data['user_id']}/{masked_token}/"
            logging.info(f"💾 Вебхук (маскированный): {masked_url}")

            data = {
                'full_webhook_url': webhook_url
            }

            result = await self._make_request(
                'POST',
                f'bitrix-token/telegram/{self.user_id}/',
                json=data
            )

            return result.get('status') == 'success'

        except Exception as e:
            logging.error(f"❌ Ошибка сохранения вебхука: {e}")
            return False

    async def delete_bitrix_webhook(self):
        """Удаление вебхука Bitrix24"""
        logging.info(f"🗑️ Удаление вебхука для пользователя {self.user_id}")
        try:
            result = await self._make_request(
                'DELETE',
                f'bitrix-token/telegram/{self.user_id}/'
            )
            return result.get('status') == 'success'
        except Exception as e:
            logging.error(f"❌ Ошибка удаления вебхука: {e}")
            return False

    async def test_bitrix_connection(self, webhook_url: str):
        """Тестирование подключения к Bitrix24"""
        # Маскируем вебхук для логирования
        webhook_data = WebhookParser.parse_webhook_url(webhook_url)
        masked_token = f"{webhook_data['webhook_token'][:4]}***" if webhook_data['webhook_token'] else "***"
        masked_url = f"{webhook_data['portal_url']}/rest/{webhook_data['user_id']}/{masked_token}/"

        logging.info(f"🔗 Тестирование подключения к Bitrix24: {masked_url}")

        try:
            async with aiohttp.ClientSession() as session:
                url = f"{webhook_url.rstrip('/')}/user.current"

                start_time = datetime.now()
                async with session.post(
                        url,
                        json={},  # Для вебхука auth уже в URL
                        timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()

                    if response.status == 200:
                        data = await response.json()
                        if 'result' in data:
                            logging.info(f"✅ Тест подключения успешен ({duration:.2f}s)")
                            return {
                                'success': True,
                                'user_info': data['result']
                            }
                        elif 'error' in data:
                            logging.error(f"❌ Тест подключения: Bitrix24 API error ({duration:.2f}s)")
                            return {
                                'success': False,
                                'error': data.get('error_description', 'Ошибка Bitrix24 API')
                            }

                    error_text = await response.text()
                    logging.error(f"❌ Тест подключения: HTTP {response.status} ({duration:.2f}s)")
                    return {
                        'success': False,
                        'error': f'HTTP {response.status}: {error_text}'
                    }

        except asyncio.TimeoutError:
            logging.error("❌ Тест подключения: Таймаут")
            return {
                'success': False,
                'error': 'Таймаут подключения к Bitrix24'
            }
        except Exception as e:
            logging.error(f"❌ Тест подключения: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


# ==================== BOT INITIALIZATION ====================
router = Router()
bot = Bot(token=Config.TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)



# ==================== HELPER FUNCTIONS ====================
def format_deal(deal: Dict) -> str:
    """Форматирование информации о сделке для вывода"""
    return f"""📊 <b>Сделка:</b> {deal.get('TITLE', 'Без названия')}
🆔 ID: {deal.get('ID')}
📈 Стадия: {deal.get('STAGE_ID', 'Не указана')}
💰 Сумма: {deal.get('OPPORTUNITY', 0)} {deal.get('CURRENCY_ID', '')}
👤 Ответственный: {deal.get('ASSIGNED_BY_ID', 'Не назначен')}
📅 Создана: {deal.get('DATE_CREATE', 'Не указана')}
📋 Вероятность: {deal.get('PROBABILITY', 'Не указана')}%
🏢 Компания: {deal.get('COMPANY_ID', 'Не указана')}
👤 Контакт: {deal.get('CONTACT_ID', 'Не указан')}"""


def format_task(task: Dict) -> str:
    """Форматирование информации о задаче для вывода"""
    priority_map = {1: '🔥 Высокий', 2: '⚠️ Средний', 3: '📄 Низкий'}
    status_map = {
        1: '📝 Создана, не просмотрена',
        2: '👁️ Просмотрена',
        3: '⚡ Выполняется',
        4: '⏸️ Ждет выполнения',
        5: '✅ Завершена',
        6: '⌛ Просрочена',
        7: '🔍 Ждет контроля'
    }

    return f"""📝 <b>Задача:</b> {task.get('TITLE', 'Без названия')}
🆔 ID: {task.get('ID')}
📊 Статус: {status_map.get(task.get('STATUS', 1), 'Не указан')}
⏰ Дедлайн: {task.get('DEADLINE', 'Не установлен')}
⚡ Приоритет: {priority_map.get(task.get('PRIORITY', 3), '📄 Низкий')}
👤 Ответственный: {task.get('RESPONSIBLE_ID', 'Не назначен')}
👥 Постановщик: {task.get('CREATED_BY', 'Не указан')}
📅 Создана: {task.get('CREATED_DATE', 'Не указана')}
📋 Описание: {task.get('DESCRIPTION', 'Без описания')[:100]}..."""


def format_lead(lead: Dict) -> str:
    """Форматирование информации о лиде для вывода"""
    return f"""🎯 <b>Лид:</b> {lead.get('TITLE', 'Без названия')}
🆔 ID: {lead.get('ID')}
📊 Статус: {lead.get('STATUS_ID', 'Не указан')}
📞 Источник: {lead.get('SOURCE_ID', 'Не указан')}
👤 Ответственный: {lead.get('ASSIGNED_BY_ID', 'Не назначен')}
📅 Создан: {lead.get('DATE_CREATE', 'Не указана')}
📧 Email: {lead.get('EMAIL', 'Не указан')}
📱 Телефон: {lead.get('PHONE', 'Не указан')}
👤 Имя: {lead.get('NAME', 'Не указано')}
👤 Фамилия: {lead.get('LAST_NAME', 'Не указана')}"""


def format_contact(contact: Dict) -> str:
    """Форматирование информации о контакте для вывода"""
    return f"""👤 <b>Контакт:</b> {contact.get('NAME', '')} {contact.get('LAST_NAME', '')}
🆔 ID: {contact.get('ID')}
📞 Телефон: {contact.get('PHONE', 'Не указан')}
📧 Email: {contact.get('EMAIL', 'Не указан')}
🏢 Компания: {contact.get('COMPANY_ID', 'Не указана')}
📅 Создан: {contact.get('DATE_CREATE', 'Не указан')}"""


def format_company(company: Dict) -> str:
    """Форматирование информации о компании для вывода"""
    return f"""🏢 <b>Компания:</b> {company.get('TITLE', 'Без названия')}
🆔 ID: {company.get('ID')}
📞 Телефон: {company.get('PHONE', 'Не указан')}
📧 Email: {company.get('EMAIL', 'Не указан')}
📍 Адрес: {company.get('ADDRESS', 'Не указан')}
📅 Создана: {company.get('DATE_CREATE', 'Не указан')}"""


def format_task_statistics(stats: Dict) -> str:
    """Форматирование статистики по задачам"""
    total = stats.get('total', 0)
    if total == 0:
        return "📊 <b>Статистика по задачам:</b>\n\nНет задач для анализа"

    completed = stats.get('completed', 0)
    in_progress = stats.get('in_progress', 0)
    overdue = stats.get('overdue', 0)
    pending = stats.get('pending', 0)
    deferred = stats.get('deferred', 0)
    awaiting_control = stats.get('awaiting_control', 0)
    supposedly_completed = stats.get('supposedly_completed', 0)

    # Общее количество активных задач (все кроме завершенных)
    active_tasks = total - completed

    completion_rate = (completed / total * 100) if total > 0 else 0
    overdue_rate = (overdue / total * 100) if total > 0 else 0
    active_rate = (active_tasks / total * 100) if total > 0 else 0

    response = f"""📊 <b>Статистика по задачам:</b>

📈 Всего задач: {total}
✅ Завершено: {completed} ({completion_rate:.1f}%)
⚡ В работе: {in_progress}
⌛ Ожидают выполнения: {pending}
⏰ Отложены: {deferred}
👁️ Ждут контроля: {awaiting_control}
📋 На проверке: {supposedly_completed}
❌ Просрочено: {overdue} ({overdue_rate:.1f}%)

<b>Состояние:</b>
📊 Активных задач: {active_tasks} ({active_rate:.1f}%)
📈 Выполнено: {completion_rate:.1f}%
🔴 Просрочено: {overdue_rate:.1f}%"""

    # Дополнительная аналитика
    if total > 0:
        # Эффективность выполнения (завершенные / все кроме новых)
        if total - pending > 0:
            efficiency = (completed / (total - pending) * 100)
            response += f"\n🏆 Эффективность: {efficiency:.1f}%"

        # Коэффициент просрочек среди активных задач
        if active_tasks > 0:
            overdue_ratio = (overdue / active_tasks * 100)
            response += f"\n⚠️ Просрочки среди активных: {overdue_ratio:.1f}%"

    return response


def get_period_dates(period: str) -> tuple:
    """Получение дат начала и конца периода"""
    today = datetime.now()
    period_lower = period.lower()

    if period_lower == 'сегодня':
        return today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    elif period_lower == 'вчера':
        yesterday = today - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d'), yesterday.strftime('%Y-%m-%d')
    elif period_lower == 'неделя':
        week_ago = today - timedelta(days=7)
        return week_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    elif period_lower == 'месяц':
        month_ago = today - timedelta(days=30)
        return month_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    elif period_lower == 'квартал':
        quarter_ago = today - timedelta(days=90)
        return quarter_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
    else:
        try:
            if ' ' in period:
                start_str, end_str = period.split(' ')
                return start_str, end_str
        except:
            pass
        return today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')


def check_bitrix_connected(webhook_data: Optional[Dict]) -> bool:
    """Проверяет, есть ли активное подключение к Bitrix24"""
    return webhook_data is not None and webhook_data.get('full_webhook_url') is not None


# ==================== COMMAND HANDLERS ====================
@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    logging.info(f"🚀 Команда /start от пользователя {message.from_user.id}")

    welcome_text = """
👋 <b>Добро пожаловать в Bitrix24 бота!</b>

<b>Используйте кнопки ниже или команды:</b>
/help - Просмотр всех команд
/auth - Привязка к Bitrix24
/status - Проверка подключения
/logout - Отвязка от Bitrix24
    """

    # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
    await message.answer(welcome_text, reply_markup=get_main_keyboard(), parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help"""
    logging.info(f"ℹ️ Команда /help от пользователя {message.from_user.id}")
    help_text = """
<b>📋 Доступные команды:</b>

<b>Основные команды:</b>
/start - Запуск бота
/help - Справка по командам
/auth - Привязка к Bitrix24
/logout - Отвязка от Bitrix24
/status - Статус подключения

<b>Чтение данных:</b>
Мои сделки - Список сделок
Мои задачи - Список задач
Сделка [ID] - Детали сделки
Задача [ID] - Детали задачи
Поиск контакта [запрос] - Поиск контакта
Поиск компании [запрос] - Поиск компании
Мои лиды - Список лидов
Отчёт по сделкам [период] - Отчёт по сделкам
Статистика по задачам - Статистика задач

<b>Создание данных:</b>
Создать лид - Новый лид
Создать сделку - Новая сделка
Создать задачу - Новая задача
Создать контакт - Новый контакт

<b>Изменение данных:</b>
Изменить сделку [ID] - Изменить сделку
Изменить задачу [ID] - Изменить задачу
Изменить лид [ID] - Изменить лид
Добавить комментарий к [ID] - Добавить комментарий
Переназначить задачу [ID] - Переназначить задачу
Изменить статус лида [ID] - Изменить статус лида

<b>Файлы:</b>
Прикрепить файл к [ID] - Прикрепить файл

<b>Отчётность:</b>
Рассчитать сумму сделок [период] - Сумма сделок

<b>Быстрые действия:</b>
Быстрая сделка - Быстрое создание сделки
"""
    await message.answer(help_text)


@router.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    """Обработчик команды /auth"""
    logging.info(f"🔗 Команда /auth от пользователя {message.from_user.id}")
    await message.answer(
        "🔗 <b>Привязка к Bitrix24</b>\n\n"
        "1. Получите вебхук в вашем Bitrix24:\n"
        "   • Зайдите в Настройки → Разработчикам → Вебхуки\n"
        "   • Создайте новый входящий вебхук\n"
        "   • Выберите права доступа\n"
        "   • Скопируйте URL вебхука\n\n"
        "2. Отправьте мне полный URL вебхука:\n"
        "   <code>https://ваш-портал.bitrix24.ru/rest/номер/токен/</code>\n\n"
        "<b>Пример:</b>\n"
        "<code>https://b24-r9de8y.bitrix24.ru/rest/10/abcdef123456/</code>",
        # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(AuthStates.waiting_webhook)


@router.message(AuthStates.waiting_webhook)
async def process_webhook(message: Message, state: FSMContext):
    """Обработка вебхука от пользователя"""
    logging.info(f"🔗 Обработка вебхука от пользователя {message.from_user.id}")

    webhook_url = message.text.strip()

    if not WebhookParser.validate_webhook_url(webhook_url):
        logging.warning(f"❌ Невалидный вебхук от пользователя {message.from_user.id}")
        await message.answer(
            "❌ <b>Некорректный формат вебхука!</b>\n\n"
            "Вебхук должен быть в формате:\n"
            "<code>https://ваш-портал.bitrix24.ru/rest/номер/токен/</code>\n\n"
            "Попробуйте снова: /auth",
            # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
            reply_markup=get_main_keyboard(),
            parse_mode="HTML"
        )
        await state.clear()
        return

    try:
        # Парсим вебхук для получения информации
        webhook_data = WebhookParser.parse_webhook_url(webhook_url)
        masked_token = f"{webhook_data['webhook_token'][:4]}***" if webhook_data['webhook_token'] else "***"
        masked_url = f"{webhook_data['portal_url']}/rest/{webhook_data['user_id']}/{masked_token}/"
        logging.info(f"🔗 Вебхук (маскированный): {masked_url}")

        backend_client = BackendAPIClient(message.from_user.id)

        test_result = await backend_client.test_bitrix_connection(webhook_url)

        if not test_result.get('success'):
            logging.error(f"❌ Ошибка тестирования подключения для пользователя {message.from_user.id}")
            await message.answer(
                f"❌ <b>Ошибка подключения</b>\n\n"
                f"{test_result.get('error', 'Неизвестная ошибка')}\n\n"
                "Проверьте вебхук и попробуйте снова: /auth",
                # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
                reply_markup=get_main_keyboard(),
                parse_mode="HTML"
            )
            await state.clear()
            return

        success = await backend_client.save_bitrix_webhook(webhook_url)

        if success:
            bitrix_user = test_result.get('user_info', {})
            logging.info(f"✅ Успешная привязка Bitrix24 для пользователя {message.from_user.id}")
            await message.answer(
                "✅ <b>Успешно привязано!</b>\n\n"
                f"🌐 Портал: {webhook_data['portal_url']}\n"
                f"👤 ID пользователя Bitrix24: {webhook_data['user_id']}\n"
                f"👤 Имя: {bitrix_user.get('NAME', 'Неизвестно')} {bitrix_user.get('LAST_NAME', '')}\n"
                f"📧 Email: {bitrix_user.get('EMAIL', 'Не указан')}\n"
                f"🆔 ID профиля: {bitrix_user.get('ID', 'Неизвестно')}\n\n"
                "Теперь вы можете использовать все функции бота!",
                # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
                reply_markup=get_main_keyboard(),
                parse_mode="HTML"
            )
        else:
            logging.error(f"❌ Ошибка сохранения вебхука для пользователя {message.from_user.id}")
            await message.answer(
                "❌ <b>Ошибка при сохранении вебхука</b>\n\n"
                "Попробуйте снова: /auth",
                # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
                reply_markup=get_main_keyboard(),
                parse_mode="HTML"
            )

    except Exception as e:
        logging.error(f"❌ Ошибка обработки вебхука для пользователя {message.from_user.id}: {e}")
        await message.answer(
            f"❌ <b>Ошибка:</b> {str(e)}\n\n"
            "Попробуйте снова: /auth",
            # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
            reply_markup=get_main_keyboard(),
            parse_mode="HTML"
        )

    await state.clear()


@router.message(Command("logout"))
async def cmd_logout(message: Message):
    """Обработчик команды /logout"""
    logging.info(f"🚪 Команда /logout от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    success = await backend_client.delete_bitrix_webhook()

    if success:
        logging.info(f"✅ Успешный логаут для пользователя {message.from_user.id}")
        # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
        await message.answer("✅ Вы успешно отвязаны от Bitrix24", reply_markup=get_main_keyboard())
    else:
        logging.error(f"❌ Ошибка логаута для пользователя {message.from_user.id}")
        # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
        await message.answer("❌ Ошибка при отвязке", reply_markup=get_main_keyboard())


@router.message(Command("status"))
async def cmd_status(message: Message):
    """Обработчик команды /status"""
    logging.info(f"📊 Команда /status от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)

    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.info(f"ℹ️ Пользователь {message.from_user.id} не привязан к Bitrix24")
        # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
        await message.answer(
            "❌ <b>Не привязано к Bitrix24</b>\n\nИспользуйте /auth для привязки",
            reply_markup=get_main_keyboard(),
            parse_mode="HTML"
        )
        return

    try:
        # Маскируем вебхук для логирования
        masked_token = f"{webhook_data['webhook_token'][:4]}***" if webhook_data['webhook_token'] else "***"
        masked_url = f"{webhook_data['portal_url']}/rest/{webhook_data['user_id']}/{masked_token}/"
        logging.info(f"🔍 Проверка статуса подключения: {masked_url}")

        test_result = await backend_client.test_bitrix_connection(webhook_data['full_webhook_url'])

        if test_result.get('success'):
            bitrix_user = test_result.get('user_info', {})
            logging.info(f"✅ Статус подключения: успешно для пользователя {message.from_user.id}")
            await message.answer(
                f"✅ <b>Привязано к Bitrix24</b>\n\n"
                f"🌐 Портал: {webhook_data['portal_url']}\n"
                f"👤 ID пользователя: {webhook_data['user_id']}\n"
                f"👤 Имя: {bitrix_user.get('NAME', 'Неизвестно')} {bitrix_user.get('LAST_NAME', '')}\n"
                f"📧 Email: {bitrix_user.get('EMAIL', 'Не указан')}\n"
                f"🆔 ID профиля: {bitrix_user.get('ID', 'Неизвестно')}",
                # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
                reply_markup=get_main_keyboard(),
                parse_mode="HTML"
            )
        else:
            logging.warning(f"⚠️ Статус подключения: ошибка для пользователя {message.from_user.id}")
            await message.answer(
                f"⚠️ <b>Подключение установлено, но недоступно</b>\n\n"
                f"🌐 Портал: {webhook_data['portal_url']}\n"
                f"👤 ID пользователя: {webhook_data['user_id']}\n"
                f"Ошибка: {test_result.get('error', 'Неизвестная ошибка')}\n\n"
                f"Попробуйте переподключиться: /auth",
                # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
                reply_markup=get_main_keyboard(),
                parse_mode="HTML"
            )
    except Exception as e:
        logging.error(f"❌ Ошибка проверки статуса для пользователя {message.from_user.id}: {e}")
        await message.answer(
            f"❌ <b>Ошибка подключения</b>\n\n{str(e)}\n\n"
            "Попробуйте переподключиться: /auth",
            # ТОЛЬКО ДОБАВИТЬ ЭТУ СТРОКУ ↓
            reply_markup=get_main_keyboard(),
            parse_mode="HTML"
        )

# ==================== ЧТЕНИЕ ДАННЫХ ====================
@router.message(F.text.startswith("Мои сделки"))
async def cmd_my_deals(message: Message):
    """Обработчик запроса списка сделок пользователя"""
    logging.info(f"📊 Запрос 'Мои сделки' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе 'Мои сделки'")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            text = message.text
            period = text.replace("Мои сделки", "").strip()

            filter_params = {}

            if period:
                start_date, end_date = get_period_dates(period)
                filter_params['>=DATE_CREATE'] = start_date
                filter_params['<=DATE_CREATE'] = end_date

            deals = await bitrix.get_deals(filter_params)

            if not deals or not deals.get('result'):
                logging.info(f"ℹ️ У пользователя {message.from_user.id} нет сделок")
                await message.answer("📭 Сделок не найдено")
                return

            response = "📊 <b>Ваши сделки:</b>\n\n"
            for deal in deals.get('result', [])[:10]:
                response += f"• {deal.get('TITLE', 'Без названия')}\n"
                response += f"  🆔 ID: {deal.get('ID')}\n"
                response += f"  📈 Стадия: {deal.get('STAGE_ID', 'Не указана')}\n"
                response += f"  💰 Сумма: {deal.get('OPPORTUNITY', 0)}\n"
                response += f"  📅 Дата: {deal.get('DATE_CREATE', 'Не указана')}\n\n"

            if len(deals.get('result', [])) > 10:
                response += f"\n📋 ... и еще {len(deals.get('result', [])) - 10} сделок"

            logging.info(
                f"✅ Успешно получено сделок для пользователя {message.from_user.id}: {len(deals.get('result', []))}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка получения сделок для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при получении сделок: {str(e)}")


@router.message(F.text.startswith("Сделка "))
async def cmd_deal_detail(message: Message):
    """Обработчик запроса деталей сделки"""
    deal_id = message.text.replace("Сделка", "").strip()
    logging.info(f"📋 Запрос деталей сделки ID: {deal_id} от пользователя {message.from_user.id}")

    if not deal_id:
        await message.answer("❌ Укажите ID сделки: Сделка [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе сделки")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.get_deal(deal_id)

            if 'error' in result:
                logging.error(
                    f"❌ Ошибка Bitrix24 при запросе сделки {deal_id}: {result.get('error_description', 'Неизвестная ошибка')}")
                await message.answer(f"❌ Ошибка Bitrix24: {result.get('error_description', 'Неизвестная ошибка')}")
                return

            deal = result.get('result', {})
            logging.info(f"✅ Успешно получена сделка ID: {deal_id} для пользователя {message.from_user.id}")
            await message.answer(format_deal(deal))

    except Exception as e:
        logging.error(f"❌ Ошибка получения сделки {deal_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при получении сделки: {str(e)}")


@router.message(F.text.startswith("Мои задачи"))
async def cmd_my_tasks(message: Message):
    """Обработчик запроса списка задач пользователя"""
    logging.info(f"📝 Запрос 'Мои задачи' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе 'Мои задачи'")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            text = message.text
            period = text.replace("Мои задачи", "").strip()

            filter_params = {}

            if period:
                start_date, end_date = get_period_dates(period)
                filter_params['>=CREATED_DATE'] = start_date
                filter_params['<=CREATED_DATE'] = end_date

            result = await bitrix.get_tasks(filter_params)


            if not result or 'tasks' not in result or not result['tasks']:
                logging.info(f"ℹ️ У пользователя {message.from_user.id} нет задач")
                await message.answer("📭 Задач не найдено")
                return

            tasks = result['tasks']
            response = "📝 <b>Ваши задачи:</b>\n\n"

            for task in tasks[:10]:
                response += f"• {task.get('TITLE', 'Без названия')}\n"
                response += f"  🆔 ID: {task.get('ID')}\n"

                status = task.get('STATUS')
                status_map = {
                    '1': '📝 Создана, не просмотрена',
                    '2': '👁️ Просмотрена',
                    '3': '⚡ Выполняется',
                    '4': '⏸️ Ждет выполнения',
                    '5': '✅ Завершена',
                    '6': '⌛ Просрочена'
                }
                status_text = status_map.get(str(status), f'Статус: {status}')
                response += f"  📊 Статус: {status_text}\n"

                deadline = task.get('DEADLINE')
                if deadline:
                    # Упрощаем формат даты
                    try:
                        deadline_date = deadline.split('T')[0]
                        response += f"  ⏰ Дедлайн: {deadline_date}\n"
                    except:
                        response += f"  ⏰ Дедлайн: {deadline}\n"
                else:
                    response += f"  ⏰ Дедлайн: Не установлен\n"

                priority = task.get('PRIORITY')
                priority_map = {'1': '🔥 Высокий', '2': '⚠️ Средний', '3': '📄 Низкий'}
                priority_text = priority_map.get(str(priority), 'Низкий')
                response += f"  ⚡ Приоритет: {priority_text}\n\n"

            if len(tasks) > 10:
                response += f"\n📋 ... и еще {len(tasks) - 10} задач"

            logging.info(f"✅ Успешно получено задач для пользователя {message.from_user.id}: {len(tasks)}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка получения задач для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при получении задач: {str(e)}")


@router.message(F.text.startswith("Задача "))
async def cmd_task_detail(message: Message):
    """Обработчик запроса деталей задачи"""
    task_id = message.text.replace("Задача", "").strip()
    logging.info(f"📋 Запрос деталей задачи ID: {task_id} от пользователя {message.from_user.id}")

    if not task_id:
        await message.answer("❌ Укажите ID задачи: Задача [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе задачи")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.get_task(task_id)

            if 'error' in result:
                logging.error(
                    f"❌ Ошибка Bitrix24 при запросе задачи {task_id}: {result.get('error_description', 'Неизвестная ошибка')}")
                await message.answer(f"❌ Ошибка Bitrix24: {result.get('error_description', 'Неизвестная ошибка')}")
                return

            task = result.get('result', {})

            # Используем обновленный форматтер
            from datetime import datetime

            priority_map = {'1': '🔥 Высокий', '2': '⚠️ Средний', '3': '📄 Низкий'}
            status_map = {
                '1': '📝 Создана, не просмотрена',
                '2': '👁️ Просмотрена',
                '3': '⚡ Выполняется',
                '4': '⏸️ Ждет выполнения',
                '5': '✅ Завершена',
                '6': '⌛ Просрочена',
                '7': '🔍 Ждет контроля'
            }

            response = f"""📝 <b>Задача:</b> {task.get('TITLE', 'Без названия')}
🆔 ID: {task.get('ID')}
📊 Статус: {status_map.get(str(task.get('STATUS', '1')), 'Не указан')}
⏰ Дедлайн: {task.get('DEADLINE', 'Не установлен')}
⚡ Приоритет: {priority_map.get(str(task.get('PRIORITY', '3')), '📄 Низкий')}
👤 Ответственный: {task.get('RESPONSIBLE_ID', 'Не назначен')}
👥 Постановщик: {task.get('CREATED_BY', 'Не указан')}
📅 Создана: {task.get('CREATED_DATE', 'Не указана')}
📋 Описание: {task.get('DESCRIPTION', 'Без описания')[:100]}..."""

            logging.info(f"✅ Успешно получена задача ID: {task_id} для пользователя {message.from_user.id}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка получения задачи {task_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при получении задачи: {str(e)}")

@router.message(F.text.startswith("Поиск контакта "))
async def cmd_search_contact(message: Message):
    """Обработчик поиска контактов"""
    query = message.text.replace("Поиск контакта", "").strip()
    logging.info(f"🔍 Поиск контакта: {query} от пользователя {message.from_user.id}")

    if not query:
        await message.answer("❌ Укажите запрос для поиска: Поиск контакта [запрос]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при поиске контакта")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.search_contacts(query)

            if not result or not result.get('result'):
                logging.info(f"ℹ️ Контакты не найдены по запросу '{query}' для пользователя {message.from_user.id}")
                await message.answer(f"📭 Контакты по запросу '{query}' не найдены")
                return

            contacts = result.get('result', [])
            response = f"🔍 <b>Результаты поиска контактов по запросу '{query}':</b>\n\n"

            for contact in contacts[:10]:
                name = contact.get('NAME', '')
                last_name = contact.get('LAST_NAME', '')
                full_name = f"{name} {last_name}".strip() or 'Без имени'

                response += f"• {full_name}\n"
                response += f"  🆔 ID: {contact.get('ID')}\n"

                phone = contact.get('PHONE')
                if phone and isinstance(phone, list) and len(phone) > 0:
                    phone_value = phone[0].get('VALUE', '') if isinstance(phone[0], dict) else phone[0]
                    response += f"  📞 Телефон: {phone_value}\n"
                else:
                    response += f"  📞 Телефон: Не указан\n"

                response += f"  📧 Email: {contact.get('EMAIL', 'Не указан')}\n\n"

            if len(contacts) > 10:
                response += f"\n📋 ... и еще {len(contacts) - 10} контактов"

            logging.info(f"✅ Найдено контактов для пользователя {message.from_user.id}: {len(contacts)}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка поиска контактов для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при поиске контактов: {str(e)}")


@router.message(F.text.startswith("Поиск компании "))
async def cmd_search_company(message: Message):
    """Обработчик поиска компаний"""
    query = message.text.replace("Поиск компании", "").strip()
    logging.info(f"🏢 Поиск компании: {query} от пользователя {message.from_user.id}")

    if not query:
        await message.answer("❌ Укажите запрос для поиска: Поиск компании [запрос]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при поиске компании")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.search_companies(query)

            if not result or not result.get('result'):
                logging.info(f"ℹ️ Компании не найдены по запросу '{query}' для пользователя {message.from_user.id}")
                await message.answer(f"📭 Компании по запросу '{query}' не найдены")
                return

            companies = result.get('result', [])
            response = f"🏢 <b>Результаты поиска компаний по запросу '{query}':</b>\n\n"

            for company in companies[:10]:
                response += f"• {company.get('TITLE', 'Без названия')}\n"
                response += f"  🆔 ID: {company.get('ID')}\n"
                response += f"  📞 Телефон: {company.get('PHONE', 'Не указан')}\n"
                response += f"  📧 Email: {company.get('EMAIL', 'Не указан')}\n"
                response += f"  📍 Адрес: {company.get('ADDRESS', 'Не указан')}\n\n"

            if len(companies) > 10:
                response += f"\n📋 ... и еще {len(companies) - 10} компаний"

            logging.info(f"✅ Найдено компаний для пользователя {message.from_user.id}: {len(companies)}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка поиска компаний для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при поиске компаний: {str(e)}")


@router.message(F.text.startswith("Мои лиды"))
async def cmd_my_leads(message: Message):
    """Обработчик запроса списка лидов пользователя"""
    logging.info(f"🎯 Запрос 'Мои лиды' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе 'Мои лиды'")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            text = message.text
            period = text.replace("Мои лиды", "").strip()

            filter_params = {}

            if period:
                start_date, end_date = get_period_dates(period)
                filter_params['>=DATE_CREATE'] = start_date
                filter_params['<=DATE_CREATE'] = end_date

            leads = await bitrix.get_leads(filter_params)

            if not leads or not leads.get('result'):
                logging.info(f"ℹ️ У пользователя {message.from_user.id} нет лидов")
                await message.answer("📭 Лидов не найдено")
                return

            response = "🎯 <b>Ваши лиды:</b>\n\n"
            for lead in leads.get('result', [])[:10]:
                response += f"• {lead.get('TITLE', 'Без названия')}\n"
                response += f"  🆔 ID: {lead.get('ID')}\n"
                response += f"  📊 Статус: {lead.get('STATUS_ID', 'Не указан')}\n"
                response += f"  📞 Источник: {lead.get('SOURCE_ID', 'Не указан')}\n"
                response += f"  📅 Дата: {lead.get('DATE_CREATE', 'Не указана')}\n\n"

            if len(leads.get('result', [])) > 10:
                response += f"\n📋 ... и еще {len(leads.get('result', [])) - 10} лидов"

            logging.info(
                f"✅ Успешно получено лидов для пользователя {message.from_user.id}: {len(leads.get('result', []))}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка получения лидов для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при получении лидов: {str(e)}")


@router.message(F.text.startswith("Статистика по задачам"))
async def cmd_task_statistics(message: Message):
    """Обработчик запроса статистики по задачам"""
    logging.info(f"📊 Запрос 'Статистика по задачам' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе статистики задач")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            stats = await bitrix.get_task_statistics()
            logging.info(f"✅ Статистика задач получена для пользователя {message.from_user.id}")
            await message.answer(format_task_statistics(stats))

    except Exception as e:
        logging.error(f"❌ Ошибка получения статистики задач для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка при получении статистики: {str(e)}")


# ==================== СОЗДАНИЕ ДАННЫХ ====================
@router.message(F.text.startswith("Создать лид"))
async def cmd_create_lead(message: Message, state: FSMContext):
    """Обработчик создания лида"""
    logging.info(f"🎯 Запрос 'Создать лид' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при создании лида")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    await message.answer(
        "🎯 <b>Создание нового лида</b>\n\n"
        "Введите имя лида:"
    )
    await state.set_state(LeadCreationStates.waiting_name)


@router.message(LeadCreationStates.waiting_name)
async def process_lead_name(message: Message, state: FSMContext):
    """Обработка имени лида"""
    logging.info(f"🎯 Ввод имени лида от пользователя {message.from_user.id}")
    await state.update_data(name=message.text)
    await message.answer("Введите телефон лида:")
    await state.set_state(LeadCreationStates.waiting_phone)


@router.message(LeadCreationStates.waiting_phone)
async def process_lead_phone(message: Message, state: FSMContext):
    """Обработка телефона лида"""
    logging.info(f"🎯 Ввод телефона лида от пользователя {message.from_user.id}")
    await state.update_data(phone=message.text)
    await message.answer("Введите источник лида:")
    await state.set_state(LeadCreationStates.waiting_source)


@router.message(LeadCreationStates.waiting_source)
async def process_lead_source(message: Message, state: FSMContext):
    """Обработка источника лида"""
    logging.info(f"🎯 Ввод источника лида от пользователя {message.from_user.id}")
    await state.update_data(source=message.text)
    await message.answer("Введите заголовок лида:")
    await state.set_state(LeadCreationStates.waiting_title)


@router.message(LeadCreationStates.waiting_title)
async def process_lead_title(message: Message, state: FSMContext):
    """Обработка заголовка лида и создание лида"""
    logging.info(f"🎯 Ввод заголовка лида от пользователя {message.from_user.id}")
    await state.update_data(title=message.text)

    user_data = await state.get_data()

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {
                'NAME': user_data.get('name', ''),
                'PHONE': [{'VALUE': user_data.get('phone', ''), 'VALUE_TYPE': 'WORK'}],
                'SOURCE_ID': user_data.get('source', 'WEB'),
                'TITLE': user_data.get('title', 'Новый лид')
            }

            result = await bitrix.create_lead(fields)

            if 'result' in result:
                logging.info(f"✅ Лид успешно создан для пользователя {message.from_user.id}, ID: {result['result']}")
                await message.answer(f"✅ Лид создан успешно!\n🆔 ID: {result['result']}")
            else:
                logging.error(f"❌ Ошибка создания лида для пользователя {message.from_user.id}: {result}")
                await message.answer(f"❌ Ошибка при создании лида: {result}")

    except Exception as e:
        logging.error(f"❌ Ошибка создания лида для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# ==================== СОЗДАНИЕ СДЕЛКИ ====================
@router.message(F.text.startswith("Создать сделку"))
async def cmd_create_deal(message: Message, state: FSMContext):
    """Обработчик создания сделки"""
    logging.info(f"💼 Запрос 'Создать сделку' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при создании сделки")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    await message.answer(
        "📊 <b>Создание новой сделки</b>\n\n"
        "Введите название сделки:"
    )
    await state.set_state(DealCreationStates.waiting_title)


@router.message(DealCreationStates.waiting_title)
async def process_deal_title(message: Message, state: FSMContext):
    """Обработка названия сделки"""
    logging.info(f"💼 Ввод названия сделки от пользователя {message.from_user.id}")
    await state.update_data(title=message.text)

    builder = InlineKeyboardBuilder()
    builder.button(text="C1: Первичный контакт", callback_data="stage_C1")
    builder.button(text="C2: Переговоры", callback_data="stage_C2")
    builder.button(text="C3: Согласование", callback_data="stage_C3")
    builder.button(text="C4: Подготовка документов", callback_data="stage_C4")
    builder.button(text="C5: Сделка заключена", callback_data="stage_C5")
    builder.button(text="C6: Сделка не состоялась", callback_data="stage_C6")
    builder.adjust(2)

    await message.answer(
        "Выберите стадию сделки:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data.startswith("stage_"))
async def process_deal_stage(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора стадии сделки"""
    stage = callback.data.replace("stage_", "")
    logging.info(f"💼 Выбор стадии сделки: {stage} от пользователя {callback.from_user.id}")
    await state.update_data(stage=stage)
    await callback.message.answer("Введите сумму сделки:")
    await state.set_state(DealCreationStates.waiting_amount)
    await callback.answer()


@router.message(DealCreationStates.waiting_amount)
async def process_deal_amount(message: Message, state: FSMContext):
    """Обработка суммы сделки"""
    logging.info(f"💼 Ввод суммы сделки от пользователя {message.from_user.id}")
    try:
        amount = float(message.text.replace(',', '.'))
        await state.update_data(amount=amount)
        await message.answer("Введите ID контакта или компании (или оставьте пустым):")
        await state.set_state(DealCreationStates.waiting_contact)
    except ValueError:
        logging.warning(f"⚠️ Неверный формат суммы сделки от пользователя {message.from_user.id}")
        await message.answer("❌ Введите корректную сумму (число)")


@router.message(DealCreationStates.waiting_contact)
async def process_deal_contact(message: Message, state: FSMContext):
    """Обработка контакта/компании и создание сделки"""
    contact_id = message.text.strip() if message.text.strip() else None
    logging.info(f"💼 Ввод контакта/компании для сделки от пользователя {message.from_user.id}: {contact_id}")
    await state.update_data(contact_id=contact_id)

    user_data = await state.get_data()

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {
                'TITLE': user_data.get('title', ''),
                'STAGE_ID': user_data.get('stage', 'NEW'),
                'OPPORTUNITY': user_data.get('amount', 0),
                'CURRENCY_ID': 'RUB'
            }

            if contact_id:
                # Определяем тип сущности по префиксу
                if contact_id.startswith('C_'):
                    fields['CONTACT_ID'] = contact_id.replace('C_', '')
                elif contact_id.startswith('CO_'):
                    fields['COMPANY_ID'] = contact_id.replace('CO_', '')
                else:
                    # Если без префикса, считаем это ID контакта
                    fields['CONTACT_ID'] = contact_id

            result = await bitrix.create_deal(fields)

            if 'result' in result:
                logging.info(
                    f"✅ Сделка успешно создана для пользователя {message.from_user.id}, ID: {result['result']}")
                await message.answer(
                    f"✅ <b>Сделка создана успешно!</b>\n\n"
                    f"🆔 ID сделки: {result['result']}\n"
                    f"📊 Название: {user_data.get('title', '')}\n"
                    f"💰 Сумма: {user_data.get('amount', 0)}\n"
                    f"📈 Стадия: {user_data.get('stage', 'NEW')}"
                )
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка создания сделки для пользователя {message.from_user.id}: {error_msg}")
                await message.answer(f"❌ Ошибка при создании сделки: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка создания сделки для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# ==================== СОЗДАНИЕ ЗАДАЧИ ====================
@router.message(F.text.startswith("Создать задачу"))
async def cmd_create_task(message: Message, state: FSMContext):
    """Обработчик создания задачи"""
    logging.info(f"📌 Запрос 'Создать задачу' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при создании задачи")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    await message.answer(
        "📌 <b>Создание новой задачи</b>\n\n"
        "Введите название задачи:"
    )
    await state.set_state(TaskCreationStates.waiting_title)


@router.message(TaskCreationStates.waiting_title)
async def process_task_title(message: Message, state: FSMContext):
    """Обработка названия задачи"""
    logging.info(f"📌 Ввод названия задачи от пользователя {message.from_user.id}")
    await state.update_data(title=message.text)
    await message.answer("Введите описание задачи (или оставьте пустым):")
    await state.set_state(TaskCreationStates.waiting_description)


@router.message(TaskCreationStates.waiting_description)
async def process_task_description(message: Message, state: FSMContext):
    """Обработка описания задачи"""
    logging.info(f"📌 Ввод описания задачи от пользователя {message.from_user.id}")
    await state.update_data(description=message.text)

    builder = InlineKeyboardBuilder()
    builder.button(text="🔥 Высокий", callback_data="priority_1")
    builder.button(text="⚠️ Средний", callback_data="priority_2")
    builder.button(text="📄 Низкий", callback_data="priority_3")
    builder.adjust(3)

    await message.answer(
        "Выберите приоритет задачи:",
        reply_markup=builder.as_markup()
    )


@router.callback_query(F.data.startswith("priority_"))
async def process_task_priority(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора приоритета задачи"""
    priority = callback.data.replace("priority_", "")
    logging.info(f"📌 Выбор приоритета задачи: {priority} от пользователя {callback.from_user.id}")
    await state.update_data(priority=int(priority))
    await callback.message.answer("Введите дедлайн задачи (в формате ГГГГ-ММ-ДД или оставьте пустым):")
    await state.set_state(TaskCreationStates.waiting_deadline)
    await callback.answer()


@router.message(TaskCreationStates.waiting_deadline)
async def process_task_deadline(message: Message, state: FSMContext):
    """Обработка дедлайна задачи"""
    deadline = message.text.strip() if message.text.strip() else None
    logging.info(f"📌 Ввод дедлайна задачи от пользователя {message.from_user.id}: {deadline}")

    user_data = await state.get_data()

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {
                'TITLE': user_data.get('title', 'Новая задача'),
                'DESCRIPTION': user_data.get('description', ''),
                'PRIORITY': user_data.get('priority', 3)
            }

            if deadline:
                try:
                    # Проверяем формат даты
                    datetime.strptime(deadline, '%Y-%m-%d')
                    fields['DEADLINE'] = deadline
                except ValueError:
                    logging.warning(f"⚠️ Неверный формат даты от пользователя {message.from_user.id}")
                    await message.answer("❌ Неверный формат даты. Используйте формат ГГГГ-ММ-ДД")
                    return

            result = await bitrix.create_task(fields)

            if 'result' in result:
                task_id = result['result']
                logging.info(f"✅ Задача успешно создана для пользователя {message.from_user.id}, ID: {task_id}")
                await message.answer(
                    f"✅ <b>Задача создана успешно!</b>\n\n"
                    f"🆔 ID задачи: {task_id}\n"
                    f"📌 Название: {user_data.get('title', '')}\n"
                    f"⚡ Приоритет: {['🔥 Высокий', '⚠️ Средний', '📄 Низкий'][user_data.get('priority', 3) - 1]}\n"
                    f"⏰ Дедлайн: {deadline if deadline else 'Не установлен'}"
                )
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка создания задачи для пользователя {message.from_user.id}: {error_msg}")
                await message.answer(f"❌ Ошибка при создании задачи: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка создания задачи для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# ==================== СОЗДАНИЕ КОНТАКТА ====================
@router.message(F.text.startswith("Создать контакт"))
async def cmd_create_contact(message: Message, state: FSMContext):
    """Обработчик создания контакта"""
    logging.info(f"👤 Запрос 'Создать контакт' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при создании контакта")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    await message.answer(
        "👤 <b>Создание нового контакта</b>\n\n"
        "Введите имя контакта:"
    )
    await state.set_state(ContactCreationStates.waiting_first_name)


@router.message(ContactCreationStates.waiting_first_name)
async def process_contact_first_name(message: Message, state: FSMContext):
    """Обработка имени контакта"""
    logging.info(f"👤 Ввод имени контакта от пользователя {message.from_user.id}")
    await state.update_data(first_name=message.text)
    await message.answer("Введите фамилию контакта (или оставьте пустым):")
    await state.set_state(ContactCreationStates.waiting_last_name)


@router.message(ContactCreationStates.waiting_last_name)
async def process_contact_last_name(message: Message, state: FSMContext):
    """Обработка фамилии контакта"""
    logging.info(f"👤 Ввод фамилии контакта от пользователя {message.from_user.id}")
    await state.update_data(last_name=message.text)
    await message.answer("Введите телефон контакта:")
    await state.set_state(ContactCreationStates.waiting_phone)


@router.message(ContactCreationStates.waiting_phone)
async def process_contact_phone(message: Message, state: FSMContext):
    """Обработка телефона контакта"""
    logging.info(f"👤 Ввод телефона контакта от пользователя {message.from_user.id}")
    await state.update_data(phone=message.text)
    await message.answer("Введите email контакта (или оставьте пустым):")
    await state.set_state(ContactCreationStates.waiting_email)


@router.message(ContactCreationStates.waiting_email)
async def process_contact_email(message: Message, state: FSMContext):
    """Обработка email контакта и создание контакта"""
    email = message.text.strip() if message.text.strip() else None
    logging.info(f"👤 Ввод email контакта от пользователя {message.from_user.id}: {email}")

    user_data = await state.get_data()

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {
                'NAME': user_data.get('first_name', ''),
                'LAST_NAME': user_data.get('last_name', ''),
            }

            # Добавляем телефон если указан
            phone = user_data.get('phone')
            if phone:
                fields['PHONE'] = [{'VALUE': phone, 'VALUE_TYPE': 'WORK'}]

            # Добавляем email если указан
            if email:
                fields['EMAIL'] = [{'VALUE': email, 'VALUE_TYPE': 'WORK'}]

            result = await bitrix.create_contact(fields)

            if 'result' in result:
                contact_id = result['result']
                logging.info(f"✅ Контакт успешно создан для пользователя {message.from_user.id}, ID: {contact_id}")

                full_name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
                if not full_name:
                    full_name = "Контакт без имени"

                await message.answer(
                    f"✅ <b>Контакт создан успешно!</b>\n\n"
                    f"🆔 ID контакта: {contact_id}\n"
                    f"👤 Имя: {full_name}\n"
                    f"📞 Телефон: {phone if phone else 'Не указан'}\n"
                    f"📧 Email: {email if email else 'Не указан'}"
                )
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка создания контакта для пользователя {message.from_user.id}: {error_msg}")
                await message.answer(f"❌ Ошибка при создании контакта: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка создания контакта для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# ==================== ИЗМЕНЕНИЕ ДАННЫХ ====================

# -------------------- ИЗМЕНЕНИЕ СДЕЛКИ --------------------
@router.message(F.text.startswith("Изменить сделку "))
async def cmd_edit_deal(message: Message, state: FSMContext):
    """Обработчик изменения сделки"""
    deal_id = message.text.replace("Изменить сделку", "").strip()
    logging.info(f"✏️ Запрос изменения сделки ID: {deal_id} от пользователя {message.from_user.id}")

    if not deal_id:
        await message.answer("❌ Укажите ID сделки: Изменить сделку [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при изменении сделки")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    # Проверяем существование сделки
    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            deal = await bitrix.get_deal(deal_id)

            if 'error' in deal:
                error_msg = deal.get('error_description', 'Сделка не найдена')
                logging.error(f"❌ Сделка {deal_id} не найдена для пользователя {message.from_user.id}")
                await message.answer(f"❌ Сделка не найдена: {error_msg}")
                return

            await state.update_data(deal_id=deal_id)

            builder = InlineKeyboardBuilder()
            builder.button(text="📊 Название", callback_data="field_TITLE")
            builder.button(text="💰 Сумма", callback_data="field_OPPORTUNITY")
            builder.button(text="📈 Стадия", callback_data="field_STAGE_ID")
            builder.button(text="👤 Ответственный", callback_data="field_ASSIGNED_BY_ID")
            builder.button(text="📋 Вероятность", callback_data="field_PROBABILITY")
            builder.button(text="📝 Комментарий", callback_data="field_COMMENTS")
            builder.adjust(2)

            await message.answer(
                f"✏️ <b>Изменение сделки ID: {deal_id}</b>\n\n"
                f"Выберите поле для изменения:",
                reply_markup=builder.as_markup()
            )
            await state.set_state(DealEditStates.waiting_field)

    except Exception as e:
        logging.error(f"❌ Ошибка при проверке сделки {deal_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.callback_query(DealEditStates.waiting_field, F.data.startswith("field_"))
async def process_deal_field(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора поля для изменения сделки"""
    field = callback.data.replace("field_", "")
    logging.info(f"✏️ Выбор поля сделки для изменения: {field} от пользователя {callback.from_user.id}")
    await state.update_data(field=field)

    field_names = {
        'TITLE': 'название',
        'OPPORTUNITY': 'сумму',
        'STAGE_ID': 'стадию',
        'ASSIGNED_BY_ID': 'ID ответственного',
        'PROBABILITY': 'вероятность (0-100)',
        'COMMENTS': 'комментарий'
    }

    field_name = field_names.get(field, field)
    await callback.message.answer(f"Введите новое значение для {field_name}:")
    await state.set_state(DealEditStates.waiting_value)
    await callback.answer()


@router.message(DealEditStates.waiting_value)
async def process_deal_value(message: Message, state: FSMContext):
    """Обработка нового значения и обновление сделки"""
    value = message.text.strip()
    logging.info(f"✏️ Ввод нового значения для сделки от пользователя {message.from_user.id}")

    user_data = await state.get_data()
    deal_id = user_data.get('deal_id')
    field = user_data.get('field')

    if not deal_id or not field:
        logging.error(f"❌ Отсутствуют данные о сделке или поле для пользователя {message.from_user.id}")
        await message.answer("❌ Ошибка: не найдены данные сделки")
        await state.clear()
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {}

            # Обрабатываем специальные поля
            if field == 'OPPORTUNITY':
                try:
                    fields[field] = float(value.replace(',', '.'))
                except ValueError:
                    await message.answer("❌ Введите корректную сумму (число)")
                    return
            elif field == 'PROBABILITY':
                try:
                    prob = int(value)
                    if 0 <= prob <= 100:
                        fields[field] = prob
                    else:
                        await message.answer("❌ Вероятность должна быть от 0 до 100")
                        return
                except ValueError:
                    await message.answer("❌ Введите число от 0 до 100")
                    return
            elif field == 'COMMENTS':
                # Для комментариев используем специальный метод
                result = await bitrix.add_comment('deal', deal_id, value)
                if 'result' in result:
                    logging.info(f"✅ Комментарий добавлен к сделке {deal_id} пользователем {message.from_user.id}")
                    await message.answer(f"✅ Комментарий добавлен к сделке {deal_id}")
                else:
                    error_msg = result.get('error_description', 'Неизвестная ошибка')
                    await message.answer(f"❌ Ошибка при добавлении комментария: {error_msg}")
                await state.clear()
                return
            else:
                fields[field] = value

            # Обновляем сделку
            result = await bitrix.update_deal(deal_id, fields)

            if 'result' in result and result['result'] is True:
                logging.info(f"✅ Сделка {deal_id} успешно обновлена пользователем {message.from_user.id}")
                await message.answer(f"✅ Сделка {deal_id} успешно обновлена!")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка обновления сделки {deal_id}: {error_msg}")
                await message.answer(f"❌ Ошибка при обновлении сделки: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка обновления сделки {deal_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# -------------------- ИЗМЕНЕНИЕ ЗАДАЧИ --------------------
@router.message(F.text.startswith("Изменить задачу "))
async def cmd_edit_task(message: Message, state: FSMContext):
    """Обработчик изменения задачи"""
    task_id = message.text.replace("Изменить задачу", "").strip()
    logging.info(f"✏️ Запрос изменения задачи ID: {task_id} от пользователя {message.from_user.id}")

    if not task_id:
        await message.answer("❌ Укажите ID задачи: Изменить задачу [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при изменении задачи")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    # Проверяем существование задачи
    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            task = await bitrix.get_task(task_id)

            if 'error' in task:
                error_msg = task.get('error_description', 'Задача не найдена')
                logging.error(f"❌ Задача {task_id} не найдена для пользователя {message.from_user.id}")
                await message.answer(f"❌ Задача не найдена: {error_msg}")
                return

            await state.update_data(task_id=task_id)

            builder = InlineKeyboardBuilder()
            builder.button(text="📌 Название", callback_data="taskfield_TITLE")
            builder.button(text="📋 Описание", callback_data="taskfield_DESCRIPTION")
            builder.button(text="⚡ Приоритет", callback_data="taskfield_PRIORITY")
            builder.button(text="⏰ Дедлайн", callback_data="taskfield_DEADLINE")
            builder.button(text="👤 Ответственный", callback_data="taskfield_RESPONSIBLE_ID")
            builder.button(text="📝 Комментарий", callback_data="taskfield_COMMENTS")
            builder.button(text="📊 Статус", callback_data="taskfield_STATUS")
            builder.adjust(2)

            await message.answer(
                f"✏️ <b>Изменение задачи ID: {task_id}</b>\n\n"
                f"Выберите поле для изменения:",
                reply_markup=builder.as_markup()
            )
            await state.set_state(TaskEditStates.waiting_field)

    except Exception as e:
        logging.error(f"❌ Ошибка при проверке задачи {task_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.callback_query(TaskEditStates.waiting_field, F.data.startswith("taskfield_"))
async def process_task_field(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора поля для изменения задачи"""
    field = callback.data.replace("taskfield_", "")
    logging.info(f"✏️ Выбор поля задачи для изменения: {field} от пользователя {callback.from_user.id}")
    await state.update_data(field=field)

    field_names = {
        'TITLE': 'название',
        'DESCRIPTION': 'описание',
        'PRIORITY': 'приоритет (1-высокий, 2-средний, 3-низкий)',
        'DEADLINE': 'дедлайн (ГГГГ-ММ-ДД)',
        'RESPONSIBLE_ID': 'ID ответственного',
        'COMMENTS': 'комментарий',
        'STATUS': 'статус (1-новая, 2-просмотрена, 3-выполняется, 5-завершена)'
    }

    field_name = field_names.get(field, field)
    await callback.message.answer(f"Введите новое значение для {field_name}:")
    await state.set_state(TaskEditStates.waiting_value)
    await callback.answer()


@router.message(TaskEditStates.waiting_value)
async def process_task_value(message: Message, state: FSMContext):
    """Обработка нового значения и обновление задачи"""
    value = message.text.strip()
    logging.info(f"✏️ Ввод нового значения для задачи от пользователя {message.from_user.id}")

    user_data = await state.get_data()
    task_id = user_data.get('task_id')
    field = user_data.get('field')

    if not task_id or not field:
        logging.error(f"❌ Отсутствуют данные о задаче или поле для пользователя {message.from_user.id}")
        await message.answer("❌ Ошибка: не найдены данные задачи")
        await state.clear()
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {}

            # Обрабатываем специальные поля
            if field == 'PRIORITY':
                try:
                    priority = int(value)
                    if priority in [1, 2, 3]:
                        fields[field] = priority
                    else:
                        await message.answer("❌ Приоритет должен быть 1, 2 или 3")
                        return
                except ValueError:
                    await message.answer("❌ Введите число 1, 2 или 3")
                    return
            elif field == 'STATUS':
                try:
                    status = int(value)
                    if status in [1, 2, 3, 5]:
                        fields[field] = status
                    else:
                        await message.answer("❌ Статус должен быть 1, 2, 3 или 5")
                        return
                except ValueError:
                    await message.answer("❌ Введите число 1, 2, 3 или 5")
                    return
            elif field == 'DEADLINE':
                try:
                    # Проверяем формат даты
                    datetime.strptime(value, '%Y-%m-%d')
                    fields[field] = value
                except ValueError:
                    await message.answer("❌ Неверный формат даты. Используйте ГГГГ-ММ-ДД")
                    return
            elif field == 'COMMENTS':
                # Для комментариев используем специальный метод
                result = await bitrix.add_comment('task', task_id, value)
                if 'result' in result:
                    logging.info(f"✅ Комментарий добавлен к задаче {task_id} пользователем {message.from_user.id}")
                    await message.answer(f"✅ Комментарий добавлен к задаче {task_id}")
                else:
                    error_msg = result.get('error_description', 'Неизвестная ошибка')
                    await message.answer(f"❌ Ошибка при добавлении комментария: {error_msg}")
                await state.clear()
                return
            elif field == 'RESPONSIBLE_ID':
                try:
                    fields[field] = int(value)
                except ValueError:
                    await message.answer("❌ Введите числовой ID пользователя")
                    return
            else:
                fields[field] = value

            # Обновляем задачу
            result = await bitrix.update_task(task_id, fields)

            if 'result' in result and result['result'] is True:
                logging.info(f"✅ Задача {task_id} успешно обновлена пользователем {message.from_user.id}")
                await message.answer(f"✅ Задача {task_id} успешно обновлена!")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка обновления задачи {task_id}: {error_msg}")
                await message.answer(f"❌ Ошибка при обновлении задачи: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка обновления задачи {task_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# -------------------- ИЗМЕНЕНИЕ ЛИДА --------------------
@router.message(F.text.startswith("Изменить лид "))
async def cmd_edit_lead(message: Message, state: FSMContext):
    """Обработчик изменения лида"""
    lead_id = message.text.replace("Изменить лид", "").strip()
    logging.info(f"✏️ Запрос изменения лида ID: {lead_id} от пользователя {message.from_user.id}")

    if not lead_id:
        await message.answer("❌ Укажите ID лида: Изменить лид [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при изменении лида")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    # Проверяем существование лида
    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            lead = await bitrix.get_lead(lead_id)

            if 'error' in lead:
                error_msg = lead.get('error_description', 'Лид не найден')
                logging.error(f"❌ Лид {lead_id} не найдена для пользователя {message.from_user.id}")
                await message.answer(f"❌ Лид не найден: {error_msg}")
                return

            await state.update_data(lead_id=lead_id)

            builder = InlineKeyboardBuilder()
            builder.button(text="🎯 Название", callback_data="leadfield_TITLE")
            builder.button(text="👤 Имя", callback_data="leadfield_NAME")
            builder.button(text="👤 Фамилия", callback_data="leadfield_LAST_NAME")
            builder.button(text="📞 Телефон", callback_data="leadfield_PHONE")
            builder.button(text="📧 Email", callback_data="leadfield_EMAIL")
            builder.button(text="📊 Статус", callback_data="leadfield_STATUS_ID")
            builder.button(text="📞 Источник", callback_data="leadfield_SOURCE_ID")
            builder.button(text="👤 Ответственный", callback_data="leadfield_ASSIGNED_BY_ID")
            builder.button(text="📝 Комментарий", callback_data="leadfield_COMMENTS")
            builder.adjust(2)

            await message.answer(
                f"✏️ <b>Изменение лида ID: {lead_id}</b>\n\n"
                f"Выберите поле для изменения:",
                reply_markup=builder.as_markup()
            )
            await state.set_state(LeadEditStates.waiting_field)

    except Exception as e:
        logging.error(f"❌ Ошибка при проверке лида {lead_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.callback_query(LeadEditStates.waiting_field, F.data.startswith("leadfield_"))
async def process_lead_field(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора поля для изменения лида"""
    field = callback.data.replace("leadfield_", "")
    logging.info(f"✏️ Выбор поля лида для изменения: {field} от пользователя {callback.from_user.id}")
    await state.update_data(field=field)

    field_names = {
        'TITLE': 'название',
        'NAME': 'имя',
        'LAST_NAME': 'фамилию',
        'PHONE': 'телефон',
        'EMAIL': 'email',
        'STATUS_ID': 'статус',
        'SOURCE_ID': 'источник',
        'ASSIGNED_BY_ID': 'ID ответственного',
        'COMMENTS': 'комментарий'
    }

    field_name = field_names.get(field, field)
    await callback.message.answer(f"Введите новое значение для {field_name}:")
    await state.set_state(LeadEditStates.waiting_value)
    await callback.answer()


@router.message(LeadEditStates.waiting_value)
async def process_lead_value(message: Message, state: FSMContext):
    """Обработка нового значения и обновление лида"""
    value = message.text.strip()
    logging.info(f"✏️ Ввод нового значения для лида от пользователя {message.from_user.id}")

    user_data = await state.get_data()
    lead_id = user_data.get('lead_id')
    field = user_data.get('field')

    if not lead_id or not field:
        logging.error(f"❌ Отсутствуют данные о лиде или поле для пользователя {message.from_user.id}")
        await message.answer("❌ Ошибка: не найдены данные лида")
        await state.clear()
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {}

            # Обрабатываем специальные поля
            if field == 'PHONE':
                fields[field] = [{'VALUE': value, 'VALUE_TYPE': 'WORK'}]
            elif field == 'EMAIL':
                fields[field] = [{'VALUE': value, 'VALUE_TYPE': 'WORK'}]
            elif field == 'ASSIGNED_BY_ID':
                try:
                    fields[field] = int(value)
                except ValueError:
                    await message.answer("❌ Введите числовой ID пользователя")
                    return
            elif field == 'COMMENTS':
                # Для комментариев используем специальный метод
                result = await bitrix.add_comment('lead', lead_id, value)
                if 'result' in result:
                    logging.info(f"✅ Комментарий добавлен к лиду {lead_id} пользователем {message.from_user.id}")
                    await message.answer(f"✅ Комментарий добавлен к лиду {lead_id}")
                else:
                    error_msg = result.get('error_description', 'Неизвестная ошибка')
                    await message.answer(f"❌ Ошибка при добавлении комментария: {error_msg}")
                await state.clear()
                return
            else:
                fields[field] = value

            # Обновляем лид
            result = await bitrix.update_lead(lead_id, fields)

            if 'result' in result and result['result'] is True:
                logging.info(f"✅ Лид {lead_id} успешно обновлен пользователем {message.from_user.id}")
                await message.answer(f"✅ Лид {lead_id} успешно обновлен!")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка обновления лида {lead_id}: {error_msg}")
                await message.answer(f"❌ Ошибка при обновлении лида: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка обновления лида {lead_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# -------------------- ДОБАВЛЕНИЕ КОММЕНТАРИЯ --------------------
@router.message(F.text.startswith("Добавить комментарий к "))
async def cmd_add_comment(message: Message, state: FSMContext):
    """Обработчик добавления комментария"""
    entity_info = message.text.replace("Добавить комментарий к", "").strip()
    logging.info(f"💬 Запрос добавления комментария к сущности: {entity_info} от пользователя {message.from_user.id}")

    if not entity_info:
        await message.answer("❌ Укажите ID сущности: Добавить комментарий к [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при добавлении комментария")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    # Определяем тип сущности по префиксу или контексту
    if 'deal' in entity_info.lower() or 'сделка' in entity_info.lower():
        entity_type = 'deal'
        entity_id = entity_info.replace('deal', '').replace('сделка', '').replace('DEAL', '').strip()
    elif 'task' in entity_info.lower() or 'задача' in entity_info.lower():
        entity_type = 'task'
        entity_id = entity_info.replace('task', '').replace('задача', '').replace('TASK', '').strip()
    elif 'lead' in entity_info.lower() or 'лид' in entity_info.lower():
        entity_type = 'lead'
        entity_id = entity_info.replace('lead', '').replace('лид', '').replace('LEAD', '').strip()
    else:
        # По умолчанию считаем, что это ID сделки
        entity_type = 'deal'
        entity_id = entity_info.strip()

    if not entity_id:
        await message.answer("❌ Укажите ID сущности: Добавить комментарий к [ID]")
        return

    await state.update_data(entity_type=entity_type, entity_id=entity_id)

    entity_names = {
        'deal': 'сделке',
        'task': 'задаче',
        'lead': 'лиду'
    }

    entity_name = entity_names.get(entity_type, 'сущности')
    await message.answer(
        f"💬 <b>Добавление комментария к {entity_name} ID: {entity_id}</b>\n\n"
        f"Введите текст комментария:"
    )
    await state.set_state(CommentStates.waiting_comment)


@router.message(CommentStates.waiting_comment)
async def process_comment_text(message: Message, state: FSMContext):
    """Обработка текста комментария и его добавление"""
    comment_text = message.text.strip()
    logging.info(f"💬 Ввод текста комментария от пользователя {message.from_user.id}")

    user_data = await state.get_data()
    entity_type = user_data.get('entity_type')
    entity_id = user_data.get('entity_id')

    if not entity_type or not entity_id:
        logging.error(f"❌ Отсутствуют данные о сущности для пользователя {message.from_user.id}")
        await message.answer("❌ Ошибка: не найдены данные сущности")
        await state.clear()
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.add_comment(entity_type, entity_id, comment_text)

            entity_names = {
                'deal': 'сделке',
                'task': 'задаче',
                'lead': 'лиду'
            }
            entity_name = entity_names.get(entity_type, 'сущности')

            if 'result' in result:
                logging.info(f"✅ Комментарий добавлен к {entity_type} {entity_id} пользователем {message.from_user.id}")
                await message.answer(f"✅ Комментарий добавлен к {entity_name} {entity_id}")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка добавления комментария к {entity_type} {entity_id}: {error_msg}")
                await message.answer(f"❌ Ошибка при добавлении комментария: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка добавления комментария для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# -------------------- ПЕРЕНАЗНАЧЕНИЕ ЗАДАЧИ --------------------
@router.message(F.text.startswith("Переназначить задачу "))
async def cmd_reassign_task(message: Message, state: FSMContext):
    """Обработчик переназначения задачи"""
    task_id = message.text.replace("Переназначить задачу", "").strip()
    logging.info(f"🔄 Запрос переназначения задачи ID: {task_id} от пользователя {message.from_user.id}")

    if not task_id:
        await message.answer("❌ Укажите ID задачи: Переназначить задачу [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при переназначении задачи")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    # Проверяем существование задачи
    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            task = await bitrix.get_task(task_id)

            if 'error' in task:
                error_msg = task.get('error_description', 'Задача не найдена')
                logging.error(f"❌ Задача {task_id} не найдена для пользователя {message.from_user.id}")
                await message.answer(f"❌ Задача не найдена: {error_msg}")
                return

            await state.update_data(task_id=task_id)
            await message.answer(
                f"🔄 <b>Переназначение задачи ID: {task_id}</b>\n\n"
                f"Введите ID нового ответственного пользователя:"
            )
            await state.set_state(TaskReassignStates.waiting_responsible)

    except Exception as e:
        logging.error(f"❌ Ошибка при проверке задачи {task_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(TaskReassignStates.waiting_responsible)
async def process_task_responsible(message: Message, state: FSMContext):
    """Обработка ID нового ответственного и переназначение задачи"""
    responsible_id = message.text.strip()
    logging.info(f"🔄 Ввод ID ответственного для задачи от пользователя {message.from_user.id}: {responsible_id}")

    user_data = await state.get_data()
    task_id = user_data.get('task_id')

    if not task_id:
        logging.error(f"❌ Отсутствуют данные о задаче для пользователя {message.from_user.id}")
        await message.answer("❌ Ошибка: не найдены данные задачи")
        await state.clear()
        return

    if not responsible_id:
        await message.answer("❌ Введите ID пользователя")
        return

    try:
        responsible_id_int = int(responsible_id)
    except ValueError:
        await message.answer("❌ Введите числовой ID пользователя")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.reassign_task(task_id, responsible_id_int)

            if 'result' in result and result['result'] is True:
                logging.info(
                    f"✅ Задача {task_id} переназначена пользователю {responsible_id_int} пользователем {message.from_user.id}")
                await message.answer(f"✅ Задача {task_id} успешно переназначена пользователю {responsible_id_int}!")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка переназначения задачи {task_id}: {error_msg}")
                await message.answer(f"❌ Ошибка при переназначении задачи: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка переназначения задачи для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# -------------------- ИЗМЕНЕНИЕ СТАТУСА ЛИДА --------------------
@router.message(F.text.startswith("Изменить статус лида "))
async def cmd_change_lead_status(message: Message, state: FSMContext):
    """Обработчик изменения статуса лида"""
    lead_id = message.text.replace("Изменить статус лида", "").strip()
    logging.info(f"🔄 Запрос изменения статуса лида ID: {lead_id} от пользователя {message.from_user.id}")

    if not lead_id:
        await message.answer("❌ Укажите ID лида: Изменить статус лида [ID]")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при изменении статуса лида")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    # Проверяем существование лида
    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            lead = await bitrix.get_lead(lead_id)

            if 'error' in lead:
                error_msg = lead.get('error_description', 'Лид не найден')
                logging.error(f"❌ Лид {lead_id} не найден для пользователя {message.from_user.id}")
                await message.answer(f"❌ Лид не найден: {error_msg}")
                return

            # Получаем доступные статусы лидов
            statuses = await bitrix.get_lead_statuses()
            await state.update_data(lead_id=lead_id)

            if statuses and 'result' in statuses:
                builder = InlineKeyboardBuilder()
                for status in statuses['result']:
                    status_id = status.get('STATUS_ID')
                    status_name = status.get('NAME')
                    if status_id and status_name:
                        builder.button(text=status_name, callback_data=f"leadstatus_{status_id}")
                builder.adjust(2)

                current_status = lead.get('result', {}).get('STATUS_ID', 'Неизвестно')
                await message.answer(
                    f"🔄 <b>Изменение статуса лида ID: {lead_id}</b>\n\n"
                    f"Текущий статус: {current_status}\n"
                    f"Выберите новый статус:",
                    reply_markup=builder.as_markup()
                )
            else:
                await message.answer(
                    f"🔄 <b>Изменение статуса лида ID: {lead_id}</b>\n\n"
                    f"Введите новый статус лида:"
                )
                await state.set_state(LeadStatusStates.waiting_status)

    except Exception as e:
        logging.error(f"❌ Ошибка при проверке лида {lead_id} для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.callback_query(F.data.startswith("leadstatus_"))
async def process_lead_status_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора статуса лида через инлайн-кнопки"""
    status_id = callback.data.replace("leadstatus_", "")
    logging.info(f"🔄 Выбор статуса лида: {status_id} от пользователя {callback.from_user.id}")

    user_data = await state.get_data()
    lead_id = user_data.get('lead_id')

    if not lead_id:
        logging.error(f"❌ Отсутствуют данные о лиде для пользователя {callback.from_user.id}")
        await callback.message.answer("❌ Ошибка: не найдены данные лида")
        await state.clear()
        return

    backend_client = BackendAPIClient(callback.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.update_lead(lead_id, {'STATUS_ID': status_id})

            if 'result' in result and result['result'] is True:
                logging.info(f"✅ Статус лида {lead_id} изменен на {status_id} пользователем {callback.from_user.id}")
                await callback.message.answer(f"✅ Статус лида {lead_id} успешно изменен на {status_id}!")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка изменения статуса лида {lead_id}: {error_msg}")
                await callback.message.answer(f"❌ Ошибка при изменении статуса лида: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка изменения статуса лида для пользователя {callback.from_user.id}: {e}")
        await callback.message.answer(f"❌ Ошибка: {str(e)}")

    await callback.answer()
    await state.clear()


@router.message(LeadStatusStates.waiting_status)
async def process_lead_status_input(message: Message, state: FSMContext):
    """Обработка ввода статуса лида вручную"""
    status_id = message.text.strip()
    logging.info(f"🔄 Ввод статуса лида: {status_id} от пользователя {message.from_user.id}")

    user_data = await state.get_data()
    lead_id = user_data.get('lead_id')

    if not lead_id:
        logging.error(f"❌ Отсутствуют данные о лиде для пользователя {message.from_user.id}")
        await message.answer("❌ Ошибка: не найдены данные лида")
        await state.clear()
        return

    if not status_id:
        await message.answer("❌ Введите статус лида")
        return

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    try:
        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            result = await bitrix.update_lead(lead_id, {'STATUS_ID': status_id})

            if 'result' in result and result['result'] is True:
                logging.info(f"✅ Статус лида {lead_id} изменен на {status_id} пользователем {message.from_user.id}")
                await message.answer(f"✅ Статус лида {lead_id} успешно изменен на {status_id}!")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка изменения статуса лида {lead_id}: {error_msg}")
                await message.answer(f"❌ Ошибка при изменении статуса лида: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка изменения статуса лида для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


# ==================== ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ====================

@router.message(F.text.startswith("Отчёт по сделкам"))
async def cmd_deal_report(message: Message):
    """Обработчик отчета по сделкам"""
    period = message.text.replace("Отчёт по сделкам", "").strip()
    logging.info(f"📈 Запрос отчета по сделкам за период '{period}' от пользователя {message.from_user.id}")

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при запросе отчета")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        period = period if period else "месяц"
        start_date, end_date = get_period_dates(period)

        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            deals = await bitrix.get_deal_report(start_date, end_date)

            if not deals or not deals.get('result'):
                logging.info(
                    f"ℹ️ Нет сделок за период {start_date} - {end_date} для пользователя {message.from_user.id}")
                await message.answer(f"📭 Сделок за период {start_date} - {end_date} не найдено")
                return

            total_amount = 0
            by_stage = {}

            response = f"📈 <b>Отчёт по сделкам</b>\n"
            response += f"📅 Период: {start_date} - {end_date}\n"
            response += f"📊 Всего сделок: {len(deals.get('result', []))}\n\n"

            for deal in deals.get('result', []):
                amount = deal.get('OPPORTUNITY', 0) or 0
                if isinstance(amount, (int, float)):
                    total_amount += amount
                stage = deal.get('STAGE_ID', 'Без стадии')

                if stage not in by_stage:
                    by_stage[stage] = {'count': 0, 'amount': 0}

                by_stage[stage]['count'] += 1
                by_stage[stage]['amount'] += amount

            response += f"💰 Общая сумма: {total_amount}\n\n"
            response += "<b>По стадиям:</b>\n"

            for stage, data in by_stage.items():
                response += f"• {stage}: {data['count']} сделок, сумма: {data['amount']}\n"

            logging.info(
                f"✅ Отчет по сделкам успешно получен для пользователя {message.from_user.id}: {len(deals.get('result', []))} сделок, сумма: {total_amount}")
            await message.answer(response)

    except Exception as e:
        logging.error(f"❌ Ошибка получения отчета по сделкам для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(F.text.startswith("Рассчитать сумму сделок"))
async def cmd_calculate_deals_sum(message: Message):
    """Обработчик расчета суммы сделок"""
    period = message.text.replace("Рассчитать сумму сделок", "").strip()
    logging.info(f"💰 Запрос расчета суммы сделок за период '{period}' от пользователя {message.from_user.id}")

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при расчете суммы сделок")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    try:
        period = period if period else "месяц"
        start_date, end_date = get_period_dates(period)

        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            total = await bitrix.calculate_deals_sum(start_date, end_date)

            logging.info(
                f"✅ Расчет суммы сделок для пользователя {message.from_user.id}: {total} за период {start_date} - {end_date}")
            await message.answer(
                f"💰 <b>Сумма сделок за период</b>\n\n"
                f"📅 Период: {start_date} - {end_date}\n"
                f"💰 Общая сумма: <b>{total}</b>"
            )

    except Exception as e:
        logging.error(f"❌ Ошибка расчета суммы сделок для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(F.text.startswith("Быстрая сделка"))
async def cmd_quick_deal(message: Message, state: FSMContext):
    """Обработчик быстрого создания сделки"""
    logging.info(f"⚡ Запрос 'Быстрая сделка' от пользователя {message.from_user.id}")
    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при быстром создании сделки")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    await message.answer(
        "⚡ <b>Быстрое создание сделки</b>\n\n"
        "Введите название сделки:"
    )
    await state.set_state(QuickDealStates.waiting_title)


@router.message(QuickDealStates.waiting_title)
async def process_quick_deal_title(message: Message, state: FSMContext):
    """Обработка названия быстрой сделки"""
    logging.info(f"⚡ Ввод названия быстрой сделки от пользователя {message.from_user.id}")
    await state.update_data(title=message.text)
    await message.answer("Введите сумму сделки:")
    await state.set_state(QuickDealStates.waiting_amount)


@router.message(QuickDealStates.waiting_amount)
async def process_quick_deal_amount(message: Message, state: FSMContext):
    """Обработка суммы быстрой сделки и создание сделки"""
    logging.info(f"⚡ Ввод суммы быстрой сделки от пользователя {message.from_user.id}")
    try:
        amount = float(message.text.replace(',', '.'))
        user_data = await state.get_data()

        backend_client = BackendAPIClient(message.from_user.id)
        webhook_data = await backend_client.get_bitrix_webhook()

        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            fields = {
                'TITLE': user_data.get('title', ''),
                'STAGE_ID': 'NEW',
                'OPPORTUNITY': amount,
                'CURRENCY_ID': 'RUB'
            }

            result = await bitrix.create_deal(fields)

            if 'result' in result:
                logging.info(
                    f"✅ Быстрая сделка успешно создана для пользователя {message.from_user.id}, ID: {result['result']}")
                await message.answer(f"✅ Быстрая сделка создана успешно!\nID: {result['result']}")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(f"❌ Ошибка создания быстрой сделки для пользователя {message.from_user.id}: {error_msg}")
                await message.answer(f"❌ Ошибка при создании сделки: {error_msg}")

    except ValueError:
        logging.warning(f"⚠️ Неверный формат суммы быстрой сделки от пользователя {message.from_user.id}")
        await message.answer("❌ Введите корректную сумму (число)")
        return
    except Exception as e:
        logging.error(f"❌ Ошибка создания быстрой сделки для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

    await state.clear()


@router.message(F.text.startswith("Прикрепить файл к "))
async def cmd_attach_file(message: Message):
    """Обработчик прикрепления файла"""
    entity_id = message.text.replace("Прикрепить файл к", "").strip()
    logging.info(f"📎 Запрос прикрепления файла к сущности {entity_id} от пользователя {message.from_user.id}")

    backend_client = BackendAPIClient(message.from_user.id)
    webhook_data = await backend_client.get_bitrix_webhook()

    if not check_bitrix_connected(webhook_data):
        logging.warning(f"❌ Пользователь {message.from_user.id} не привязан к Bitrix24 при прикреплении файла")
        await message.answer("❌ Сначала привяжитесь к Bitrix24: /auth")
        return

    if not message.document and not message.photo:
        logging.warning(f"⚠️ Пользователь {message.from_user.id} не прикрепил файл")
        await message.answer("❌ Отправьте файл или фото для прикрепления")
        return

    try:
        if not entity_id:
            logging.warning(f"⚠️ Пользователь {message.from_user.id} не указал ID сущности")
            await message.answer("❌ Укажите ID сущности")
            return

        # Определяем тип сущности
        if 'deal' in entity_id.lower() or 'сделка' in entity_id.lower():
            entity_type = 'deal'
            clean_entity_id = entity_id.replace('deal', '').replace('сделка', '').replace('DEAL', '').strip()
        elif 'task' in entity_id.lower() or 'задача' in entity_id.lower():
            entity_type = 'task'
            clean_entity_id = entity_id.replace('task', '').replace('задача', '').replace('TASK', '').strip()
        elif 'lead' in entity_id.lower() or 'лид' in entity_id.lower():
            entity_type = 'lead'
            clean_entity_id = entity_id.replace('lead', '').replace('лид', '').replace('LEAD', '').strip()
        else:
            # По умолчанию считаем, что это сделка
            entity_type = 'deal'
            clean_entity_id = entity_id

        async with BitrixAPIClient(webhook_data['full_webhook_url'], webhook_data['user_id']) as bitrix:
            if message.document:
                filename = message.document.file_name
                logging.info(f"📎 Прикрепление документа '{filename}' к сущности {clean_entity_id}")
                file = await bot.get_file(message.document.file_id)
                file_data = await bot.download_file(file.file_path)
            elif message.photo:
                logging.info(f"📎 Прикрепление фото к сущности {clean_entity_id}")
                file = await bot.get_file(message.photo[-1].file_id)
                file_data = await bot.download_file(file.file_path)
                filename = f"photo_{clean_entity_id}.jpg"
            else:
                logging.warning(f"⚠️ Неподдерживаемый тип файла от пользователя {message.from_user.id}")
                await message.answer("❌ Неподдерживаемый тип файла")
                return

            result = await bitrix.attach_file(entity_type, clean_entity_id, file_data, filename)

            if 'result' in result:
                logging.info(
                    f"✅ Файл успешно прикреплен к сущности {clean_entity_id} пользователем {message.from_user.id}")
                await message.answer(f"✅ Файл успешно прикреплён к сущности {clean_entity_id}")
            else:
                error_msg = result.get('error_description', str(result))
                logging.error(
                    f"❌ Ошибка прикрепления файла к сущности {clean_entity_id} пользователем {message.from_user.id}: {error_msg}")
                await message.answer(f"❌ Ошибка при прикреплении файла: {error_msg}")

    except Exception as e:
        logging.error(f"❌ Ошибка прикрепления файла для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(F.text == "/help")
async def process_help_button(message: Message):
    await cmd_help(message)

@router.message(F.text == "/auth")
async def process_auth_button(message: Message):
    await cmd_auth(message)

@router.message(F.text == "/status")
async def process_status_button(message: Message):
    await cmd_status(message)

@router.message(F.text == "/logout")
async def process_logout_button(message: Message):
    await cmd_logout(message)

@router.message(F.text == "/start")
async def process_start_button(message: Message):
    await cmd_start(message)

# ==================== MAIN FUNCTION ====================
async def main():
    """Основная функция запуска бота"""
    # Настройка логирования с цветами и информативностью
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('bot.log', encoding='utf-8')
        ]
    )

    # Устанавливаем уровень логирования для aiohttp
    logging.getLogger('aiohttp').setLevel(logging.WARNING)

    # Информация о запуске
    logging.info("🚀 Запуск Bitrix24 Telegram Bot")
    logging.info("📝 Логирование запросов настроено")
    logging.info("🔒 Токены вебхуков маскируются в логах")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())