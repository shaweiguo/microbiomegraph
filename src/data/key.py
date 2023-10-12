import time
import threading
from pynput import keyboard


def on_press(key):
    try:
        print('alphanumeric key {0} pressed'.format(
            key.char))
    except AttributeError:
        print('special key {0} pressed'.format(
            key))


def on_release(key):
    print('{0} released'.format(key))
    if key == keyboard.Key.esc:
        # Stop listener
        return False


def esc_exit():
    # Collect events until released
    with keyboard.Listener(
            on_press=on_press,
            on_release=on_release) as listener:
        listener.join()


def esc_exit_thread():
    with keyboard.Listener(on_release=on_release) as listener:
        thread2 = threading.Thread(target=printing, args=(), daemon=True)
        thread2.start()
        listener.join()


def printing():
    while True:
        print('foo bar')
        time.sleep(2)


def main():
    esc_exit_thread()


if __name__ == '__main__':
    main()
