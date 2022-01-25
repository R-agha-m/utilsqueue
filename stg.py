from os import environ
from utils_common.global_stg_loader import global_stg_loader

GlobalStg = global_stg_loader(current_stg_address=__file__,
                              stg_class_name="LocalStg")


class LocalStg:
    _ip = None
    _debug_mode = None
    _broker = None
    _report = None
    _time_out = None

    @property
    def IP(self):
        if self._ip is None:
            from utils_common.get_public_ip import GetPublicIp
            self._ip = GetPublicIp().perform()
        return self._ip

    @property
    def BROKER(self):
        if self._broker is None:
            self._broker = {'username': 'Admin',
                            'password': 'Admin211234',
                            'virtual_host': 'defhost',
                            'host': '31.7.69.200',
                            'port': '5672',
                            "consumer_queue_name": self.IP,
                            "publish_queue_name": self.IP,
                            "prefetch_count": 1,
                            "is_durable": True,
                            "heartbeat": 1 * 60 * 60}
            return self._broker

    @property
    def DEBUG_MODE(self):
        if self._debug_mode is None:
            from utils_common.detect_boolean import detect_boolean
            self._debug_mode = detect_boolean(
                environ.get("DEBUG_MODE",
                            True))
        return self._debug_mode

    @property
    def report(self):
        if self._debug_mode is None:
            from utils_logging.get_or_create_logger import get_or_create_logger
            self._report = get_or_create_logger(
                destinations=("console",),
                level=10 if self.DEBUG_MODE else 20
            )
        return self._report

    @property
    def TIME_OUT(self):
        if self._time_out is None:
            self._time_out = 10
        return self._time_out


class StgClass(GlobalStg, LocalStg):
    pass


STG = StgClass()
report = STG.report

if __name__ == "__main__":
    print(STG.DEBUG_MODE)
    print(STG.report)
