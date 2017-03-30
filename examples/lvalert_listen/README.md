this is an example of how you can connect lvalertTest_overseer and lvalert_listen to use the actual LVAlert servers in connection with your local FakeDb, etc.

To run this example, make sure you've set up your environment so that both lvalertTest and lvalertMP are discoverable. Then simply run

    ./doit

This script contains helpful comments that should explain how the procedure is conducted.
Be warned, you may need to modify `./doit` to point to an LVAlert pubsub node which you own.
