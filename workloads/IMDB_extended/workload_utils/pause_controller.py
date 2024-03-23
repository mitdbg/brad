from typing import List
import numpy as np


class PauseController:
    # controlling the pause and resume of clients
    def __init__(
        self,
        total_num_clients: int,
        pause_semaphore: List[mp.Semaphore],  # type: ignore
        resume_semaphore: List[mp.Semaphore],  # type: ignore
    ):
        self.total_num_clients = total_num_clients
        self.pause_semaphore = pause_semaphore
        self.resume_semaphore = resume_semaphore
        self.paused_clients: List[int] = []
        self.running_clients: List[int] = list(range(total_num_clients))
        self.num_running_clients: int = total_num_clients

    def adjust_num_running_clients(
        self, num_clients: int, verbose: bool = True
    ) -> None:
        if num_clients > self.total_num_clients:
            print(
                f"invalid input number of clients {num_clients}, larger than total number of clients"
            )
            return
        if num_clients == self.num_running_clients:
            return
        elif num_clients < self.num_running_clients:
            pause_clients = np.random.choice(
                self.running_clients,
                size=self.num_running_clients - num_clients,
                replace=False,
            )
            for i in pause_clients:
                assert (
                    i not in self.paused_clients
                ), f"trying to pause a client that is already paused: {i}"
                if verbose:
                    print(f"pausing client {i}")
                self.pause_semaphore[i].release()
                self.pause_semaphore[i].release()
                self.paused_clients.append(i)
                self.running_clients.remove(i)
                self.num_running_clients -= 1
        else:
            resume_clients = np.random.choice(
                self.paused_clients,
                size=num_clients - self.num_running_clients,
                replace=False,
            )
            for i in resume_clients:
                assert (
                    i not in self.running_clients
                ), f"trying to resume a running client: {i}"
                if verbose:
                    print(f"resuming client {i}")
                self.resume_semaphore[i].release()
                self.resume_semaphore[i].release()
                self.paused_clients.remove(i)
                self.running_clients.append(i)
                self.num_running_clients += 1


def get_command_line_input(pause_controller: PauseController) -> None:
    while True:
        try:
            user_input = input()
            if user_input.isnumeric():
                num_client = int(user_input)
                pause_controller.adjust_num_running_clients(num_client)
            elif user_input == "exit":
                break
        except KeyboardInterrupt:
            break
