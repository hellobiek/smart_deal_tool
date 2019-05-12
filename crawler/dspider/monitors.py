import sys
from os.path import abspath, dirname
sys.path.insert(0, dirname(dirname(abspath(__file__))))
sys.path.insert(0, dirname(dirname(dirname(abspath(__file__)))))
import const as ct
from base.wechat import SendWechat
from spidermon import Monitor, MonitorSuite, monitors
from spidermon.contrib.monitors.mixins.stats import StatsMonitorMixin
@monitors.name('Item')
class ItemCountMonitor(Monitor):
    @monitors.name('Counter')
    def test_minimum_number_of_items(self):
        item_extracted = getattr(self.data.stats, 'item_scraped_count', 0)
        minimum_threshold = 10
        msg = 'Extracted less than {} items'.format(minimum_threshold)
        self.assertTrue(item_extracted >= minimum_threshold, msg=msg)

@monitors.name('Validation')
class ItemValidationMonitor(Monitor, StatsMonitorMixin):
    @monitors.name('NoErrors')
    def test_no_item_validation_errors(self):
        validation_errors = getattr(self.stats, 'spidermon/validation/fields/errors', 0)
        self.assertEqual(validation_errors, 0, msg='Found validation errors in {} fields'.format(validation_errors))

@monitors.name('FinishReason')
class FinishReasonMonitor(Monitor):
    @monitors.name('Expected')
    def test_should_finish_with_expected_reason(self):
        expected_reasons = ct.SPIDERMON_EXPECTED_FINISH_REASONS
        finished_reason = getattr(self.data.stats, 'finish_reason')
        msg = 'Finished with {}, the expected reasons are {}'.format(finished_reason, expected_reasons)
        self.assertTrue(finished_reason in expected_reasons, msg=msg)

@monitors.name('UnknownHttpStatus')
class UnwantedHTTPCodesMonitor(Monitor):
    @monitors.name('Code')
    def test_check_unwanted_http_codes(self):
        error_codes = ct.DEFAULT_ERROR_CODES
        for code, max_errors in error_codes.items():
            code = int(code)
            count = getattr(self.data.stats, 'downloader/response_status_count/{}'.format(code), 0)
            msg = (
                'Found {} Responses with status code = {} - '.format(count, code),
                'This exceeds the limit of {}'.format(max_errors)
            )
            self.assertTrue(count <= max_errors, msg=msg)

@monitors.name('PeriodicStats')
class PeriodicJobStatsMonitor(Monitor, StatsMonitorMixin):
    @monitors.name('MaximumErrors')
    def test_number_of_errors(self):
        accepted_num_errors = 6
        num_errors = self.data.stats.get('log_count/ERROR', 0)
        msg = 'The job has exceeded the maximum number of errors'
        self.assertLessEqual(num_errors, accepted_num_errors, msg=msg)

class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [
        FinishReasonMonitor,
        ItemValidationMonitor,
        UnwantedHTTPCodesMonitor,
    ]
    monitors_failed_actions = [
        SendWechat,
    ]
