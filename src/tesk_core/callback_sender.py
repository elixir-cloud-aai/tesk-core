import logging

import requests


class CallbackSender:
    """Implemention for the callback sender class."""

    def __init__(self, task_id="", url=""):
        self.url = url
        self.task_id = task_id

    def send(self, state):
        """Send the state to the callback receiver.
        Args:
            state (str): Descriptor of the state of the task.
        Returns:
            response (requests.models.Response): Response from the callback receiver.
            None: if the callback receiver is not set or some error occurs.
        """

        if not self.url:
            return None
        sent = False
        retries = 0
        response = None
        while not sent:
            try:
                data = {"id": self.task_id, "state": state}
                headers = {"Content-Type": "application/json"}
                response = requests.post(self.url, json=data, headers=headers)
                sent = True
            except requests.exceptions.Timeout:
                retries += 1
                if retries > 3:
                    logging.error("Callback Timeout")
                    break
                continue
            except requests.exceptions.TooManyRedirects as err:
                logging.error("Bad URL: %s", err)
                break
            except requests.exceptions.RequestException as err:
                logging.error(err)
                break

        return response
