from django.test import TestCase
from django.contrib.auth import get_user_model
from django.utils import timezone
from .models import UserActionLog
from .utils import assign_logical_sessions
import uuid
from datetime import timedelta

class LogicalSessionAssignmentTest(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username='testuser', password='123')
        self.other = User.objects.create_user(username='other', password='123')

    def test_gap_based_assignment(self):
        now = timezone.now()
        # 3 действия: 0ч, +2ч, +7ч (gap=5, разрыв между 2 и 3 = 5ч)
        log1 = UserActionLog.objects.create(user=self.user, action_type='LOGIN', action_time=now)
        log2 = UserActionLog.objects.create(user=self.user, action_type='SEARCH', action_time=now + timedelta(hours=2))
        log3 = UserActionLog.objects.create(user=self.user, action_type='LOGOUT', action_time=now + timedelta(hours=7))
        assign_logical_sessions(self.user, gap_hours=5)
        log1.refresh_from_db()
        log2.refresh_from_db()
        log3.refresh_from_db()
        print('log1:', log1.action_time, log1.logical_session_id)
        print('log2:', log2.action_time, log2.logical_session_id)
        print('log3:', log3.action_time, log3.logical_session_id)
        self.assertEqual(log1.logical_session_id, log2.logical_session_id)
        self.assertNotEqual(log1.logical_session_id, log3.logical_session_id)

    def test_edge_case_exact_gap(self):
        now = timezone.now()
        log1 = UserActionLog.objects.create(user=self.user, action_type='LOGIN', action_time=now)
        log2 = UserActionLog.objects.create(user=self.user, action_type='LOGOUT', action_time=now + timedelta(hours=5))
        assign_logical_sessions(self.user, gap_hours=5)
        log1.refresh_from_db()
        log2.refresh_from_db()
        # Ровно gap — теперь считается новой сессией
        self.assertNotEqual(log1.logical_session_id, log2.logical_session_id)

    def test_different_users(self):
        now = timezone.now()
        log1 = UserActionLog.objects.create(user=self.user, action_type='LOGIN', action_time=now)
        log2 = UserActionLog.objects.create(user=self.other, action_type='LOGIN', action_time=now)
        assign_logical_sessions(self.user, gap_hours=5)
        assign_logical_sessions(self.other, gap_hours=5)
        log1.refresh_from_db()
        log2.refresh_from_db()
        self.assertIsNotNone(log1.logical_session_id)
        self.assertIsNotNone(log2.logical_session_id)
        self.assertNotEqual(log1.logical_session_id, log2.logical_session_id)
