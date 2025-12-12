from prefect import flow, task

@task
def say_hello(name: str):
    print(f"Ciao {name}!")

@flow
def hello_flow(name: str):
    say_hello(name)

if __name__ == "__main__":
    hello_flow("Mondo")
