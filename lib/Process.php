<?php

namespace Amp\Process;

use Amp\Deferred;
use AsyncInterop\{ Loop, Promise };

class Process {
    /** @var resource|null */
    private $process;

    /** @var string */
    private $command;

    /** @var string */
    private $cwd = "";

    /** @var array */
    private $env = [];

    /** @var array */
    private $options;

    /** @var resource|null */
    private $stdin;

    /** @var resource|null */
    private $stdout;

    /** @var resource|null */
    private $stderr;

    /** @var \Amp\Deferred|int */
    private $pid;

    /** @var int */
    private $oid = 0;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var string */
    private $watcher;

    /** @var bool */
    private $running = false;

    /**
     * @param   string|array $command Command to run.
     * @param   string|null $cwd Working directory or use an empty string to use the working directory of the current
     *     PHP process.
     * @param   mixed[] $env Environment variables or use an empty array to inherit from the current PHP process.
     * @param   mixed[] $options Options for proc_open().
     */
    public function __construct($command, string $cwd = null, array $env = [], array $options = []) {
        if (\is_array($command)) {
            $command = \implode(" ", \array_map("escapeshellarg", $command));
        }
        $this->command = $command;
        $this->cwd = $cwd ?? "";

        $this->pid = new Deferred;

        foreach ($env as $key => $value) {
            if (\is_array($value)) {
                throw new \Error("\$env cannot accept array values");
            }

            $this->env[(string) $key] = (string) $value;
        }

        $this->options = $options;
    }

    /**
     * Stops the process if it is still running.
     */
    public function __destruct() {
        if (\getmypid() === $this->oid) {
            $this->kill(); // Will only terminate if the process is still running.
        }

        Loop::cancel($this->watcher);

        if (\is_resource($this->process)) {
            \proc_close($this->process);
        }

        if (\is_resource($this->stdin)) {
            \fclose($this->stdin);
        }

        if (\is_resource($this->stdout)) {
            \fclose($this->stdout);
        }

        if (\is_resource($this->stderr)) {
            \fclose($this->stderr);
        }
    }

    /**
     * Resets process values.
     */
    public function __clone() {
        $this->process = null;
        $this->deferred = null;
        $this->watcher = null;
        $this->pid = 0;
        $this->oid = 0;
        $this->stdin = null;
        $this->stdout = null;
        $this->stderr = null;
        $this->running = false;
    }

    /**
     * @throws \Amp\Process\ProcessException If starting the process fails.
     * @throws \Amp\Process\StatusError If the process is already running.
     *
     * @return \AsyncInterop\Promise<int> Succeeds with exit code of the process or fails if the process is killed.
     */
    public function execute(): Promise {
        if ($this->deferred !== null) {
            throw new StatusError("The process has already been started");
        }

        $this->deferred = $deferred = new Deferred;
        $promise = $deferred->promise();

        $fd = [
            ["pipe", "r"], // stdin
            ["pipe", "w"], // stdout
            ["pipe", "w"], // stderr
            ["pipe", "w"], // exit code pipe
        ];

        if (\strncasecmp(\PHP_OS, "WIN", 3) === 0) {
            $command = $this->command;
        } else {
            $command = \sprintf(
                '{ (%s) <&3 3<&- 3>/dev/null & } 3<&0;' .
                'pid=$!; echo $pid >&3; wait $pid; RC=$?; echo $RC >&3; exit $RC',
                $this->command
            );
        }

        $this->process = @\proc_open($command, $fd, $pipes, $this->cwd ?: null, $this->env ?: null, $this->options);

        if (!\is_resource($this->process)) {
            $message = "Could not start process";
            if ($error = \error_get_last()) {
                $message .= \sprintf(" Errno: %d; %s", $error["type"], $error["message"]);
            }
            $exception = new ProcessException($message);
            $this->pid->fail($exception);
            $this->deferred->fail($exception);
            return $promise;
        }

        $this->oid = \getmypid();
        $status = \proc_get_status($this->process);

        if (!$status) {
            \proc_close($this->process);
            $this->process = null;
            $exception = new ProcessException("Could not get process status");
            $this->pid->fail($exception);
            $this->deferred->fail($exception);
            return $promise;
        }

        $this->running = true;

        foreach ($pipes as $pipe) {
            \stream_set_blocking($pipe, false);
        }

        $this->stdin = $stdin = $pipes[0];
        $this->stdout = $pipes[1];
        $this->stderr = $pipes[2];
        $stream = $pipes[3];

        $running = &$this->running;
        $promise->when(static function () use (&$running, $stream, $stdin) {
            $running = false;
            if (\is_resource($stream)) {
                \fclose($stream);
            }
            if (\is_resource($stdin)) {
                \fclose($stdin);
            }
        });

        $pid = &$this->pid;
        $this->watcher = Loop::onReadable($stream, static function ($watcher, $resource) use (&$pid, $deferred, $stdin) {
            static $initial = true;

            try {
                if (!\is_resource($resource) || \feof($resource)) {
                    throw new ProcessException("Process ended unexpectedly");
                }

                if ($initial) {
                    $initial = false;
                    $pid->resolve((int) \rtrim(@\fread($resource, 5))); // Two bytes written as string
                    return;
                }

                Loop::cancel($watcher);

                $deferred->resolve((int) \rtrim(@\fread($resource, 3))); // Single byte written as string
            } catch (\Throwable $exception) {
                Loop::cancel($watcher);
                if ($initial) {
                    $pid->fail($exception);
                }
                $deferred->fail($exception);
            }
        });

        return $promise;
    }

    /**
     * {@inheritdoc}
     */
    public function kill() {
        if ($this->running && \is_resource($this->process)) {
            $this->running = false;

            // Forcefully kill the process using SIGKILL.
            \proc_terminate($this->process, 9);

            Loop::cancel($this->watcher);

            $this->deferred->fail(new ProcessException("The process was killed"));
        }
    }

    /**
     * Sends the given signal to the process.
     *
     * @param int $signo Signal number to send to process.
     *
     * @throws \Amp\Process\StatusError If the process is not running.
     */
    public function signal(int $signo) {
        if (!$this->isRunning()) {
            throw new StatusError("The process is not running");
        }

        \proc_terminate($this->process, $signo);
    }

    /**
     * Returns the PID of the child process. Value is only meaningful if PHP was not compiled with --enable-sigchild.
     *
     * @return \AsyncInterop\Promise<int>
     */
    public function getPid(): Promise {
        return $this->pid->promise();
    }

    /**
     * Returns the command to execute.
     *
     * @return string The command to execute.
     */
    public function getCommand(): string {
        return $this->command;
    }

    /**
     * Gets the current working directory.
     *
     * @return string The current working directory an empty string if inherited from the current PHP process.
     */
    public function getWorkingDirectory(): string {
        if ($this->cwd === "") {
            return \getcwd() ?: "";
        }

        return $this->cwd;
    }

    /**
     * Gets the environment variables array.
     *
     * @return string[] Array of environment variables.
     */
    public function getEnv(): array {
        return $this->env;
    }

    /**
     * Gets the options to pass to proc_open().
     *
     * @return mixed[] Array of options.
     */
    public function getOptions(): array {
        return $this->options;
    }

    /**
     * Determines if the process is still running.
     *
     * @return bool
     */
    public function isRunning(): bool {
        return $this->running;
    }

    /**
     * Gets the process input stream (STDIN).
     *
     * @return resource
     *
     * @throws \Amp\Process\StatusError If the process is not running.
     */
    public function getStdin() {
        if ($this->stdin === null) {
            throw new StatusError("The process has not been started");
        }

        return $this->stdin;
    }

    /**
     * Gets the process output stream (STDOUT).
     *
     * @return resource
     *
     * @throws \Amp\Process\StatusError If the process is not running.
     */
    public function getStdout() {
        if ($this->stdout === null) {
            throw new StatusError("The process has not been started");
        }

        return $this->stdout;
    }

    /**
     * Gets the process error stream (STDERR).
     *
     * @return resource
     *
     * @throws \Amp\Process\StatusError If the process is not running.
     */
    public function getStderr() {
        if ($this->stderr === null) {
            throw new StatusError("The process has not been started");
        }

        return $this->stderr;
    }
}
