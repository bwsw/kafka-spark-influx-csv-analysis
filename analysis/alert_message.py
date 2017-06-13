import json
import random
from datetime import datetime

from kafka import KafkaProducer
from singelton.singleton import Singleton



class SingleKafkaProducer(KafkaProducer, metaclass=Singleton):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_var = random.randint(0, 100)


class IAllertMessage(object):
    def __init__(self, config):
        self._config = config

    def send_message(self, **kwargs):
        raise NotImplementedError("send_message method should be overrided!")


class AlertMessageFactory(object):
    def __init__(self, config):
        self._config = config

    def instance_alert(self):
        if self._config["analysis"]["alert"]["method"] == "stdout":
            return StdOutAlert(self._config)
        elif self._config["analysis"]["alert"]["method"] == "kafka":
            return KafkaOutAlert(self._config)


class StdOutAlert(IAllertMessage, metaclass=Singleton):
    def send_message(self, **kwargs):
        string = ''
        parameters = kwargs["param"]
        if parameters["key"]:
            string = string + " Parameter '{}' for key={} out of range [{},{}] and equal {}".format(
                parameters["field"],
                parameters["key"],
                parameters["lower_bound"],
                parameters["upper_bound"],
                parameters["value"])
        else:
            string = string + " Parameter '{}' out of range [{},{}] and equal {}".format(
                parameters["field"],
                parameters["lower_bound"],
                parameters["upper_bound"],
                parameters["value"])
        string = kwargs["AnalysisModule"] + ": Time: {}".format(
            datetime.fromtimestamp(int(kwargs["timestamp"]))) + "." + string
        print(string)


class KafkaOutAlert(IAllertMessage, metaclass=Singleton):
    def __init__(self, config):
        super().__init__(config)
        self._topic = config["analysis"]["alert"]["option"]["topic"]

    def send_message(self, **kwargs):
        producer = SingleKafkaProducer(bootstrap_servers=self._config["analysis"]["alert"]["option"]["server"] + ":" +
                                                         str(self._config["analysis"]["alert"]["option"]["port"]))
        producer.send(self._topic, str.encode(json.dumps(kwargs)))
        producer.flush()
