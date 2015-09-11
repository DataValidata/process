<?php

namespace Amp;

class Process implements Promise {
	private $proc;

	private $stdin;
	private $stdout;
	private $stderr;

	private $writeDeferreds = [];
	private $writeBuf;
	private $writeTotal;
	private $writeCur;

	const BUFFER_NONE = 0;
	const BUFFER_STDOUT = 1;
	const BUFFER_STDERR = 2;
	const BUFFER_ALL = 3;

	use Placeholder;

	/**
	 * @param $cmd  string command to be executed
	 * @param $options array passed directly to proc_open.
	 *        "cwd" and "env" entries are passed as fourth respectively fifth parameters to proc_open().
	 *        "buffer" entry must be one of the self::BUFFER_* constants. Determines whether it will buffer the stdout and/or stderr data internally.
	 * @return Promise is updated with ["out", $data] or ["err", $data] for data received on stdout or stderr
	 * That Promise will be resolved to a stdClass object with stdout, stderr (when $buffer is true), exit (holding exit code) and signal (only present when terminated via signal) properties
	 */
	public function __construct($cmd, array $options = []) {
		$buffer = !isset($options["buffer"]) ? self::BUFFER_NONE : $options["buffer"];

		$fds = [["pipe", "r"], ["pipe", "w"], ["pipe", "w"]];
		$cwd = isset($options["cwd"]) ? $options["cwd"] : NULL;
		$env = isset($options["env"]) ? $options["env"] : NULL;
		if (!$this->proc = @proc_open($cmd, $fds, $pipes, $cwd, $env, $options)) {
			$this->resolve(new \RuntimeException("Failed executing command: $cmd"));
			return;
		}

		$this->writeBuf = "";
		$this->writeTotal = 0;
		$this->writeCur = 0;

		stream_set_blocking($pipes[0], false);
		stream_set_blocking($pipes[1], false);
		stream_set_blocking($pipes[2], false);

		$this->deferred = new Deferred;
		$result = new \stdClass;

		if ($buffer & self::BUFFER_STDOUT) {
			$result->stdout = "";
		}
		if ($buffer & self::BUFFER_STDERR) {
			$result->stderr = "";
		}

		$this->stdout = \Amp\onReadable($pipes[1], function($watcher, $sock) use ($result) {
			if ("" == $data = @fread($sock, 8192)) {
				\Amp\cancel($watcher);
				\Amp\cancel($this->stdin);
				\Amp\immediately(function() use ($result) {
					$status = proc_get_status($this->proc);
					\assert($status["running"] === false);
					if ($status["signaled"]) {
						$result->signal = $status["termsig"];
					}
					$result->exit = $status["exitcode"];
					$this->proc = NULL;
					$this->resolve(null, $result);

					foreach ($this->writeDeferreds as $deferred) {
						$deferred->fail(new \Exception("Write could not be completed, process finished"));
					}
					$this->writeDeferreds = [];
				});
			} else {
				isset($result->stdout) && $result->stdout .= $data;
				$this->update(["out", $data]);
			}
		});
		$this->stderr = \Amp\onReadable($pipes[2], function($watcher, $sock) use ($result) {
			if ("" == $data = @fread($sock, 8192)) {
				\Amp\cancel($watcher);
			} else {
				isset($result->stderr) && $result->stderr .= $data;
				$this->update(["err", $data]);
			}
		});
		$this->stdin = \Amp\onWritable($pipes[0], function($watcher, $sock) {
			$this->writeCur += @fwrite($sock, $this->writeBuf);

			if ($this->writeCur == $this->writeTotal) {
				\Amp\disable($watcher);
			}

			while (($next = key($this->writeDeferreds)) !== null && $next <= $this->writeCur) {
				$this->writeDeferreds[$next]->succeed($this->writeCur);
				unset($this->writeDeferreds[$next]);
			}
		}, ["enable" => false]);
	}

	/* Only kills process, Promise returned by exec() will succeed in the next tick */
	public function kill($signal = 15) {
		if (!$this->proc) {
			throw new \RuntimeException("Process already terminated");
		}
		return proc_terminate($this->proc, $signal);
	}

	/* Aborts all watching completely and immediately */
	public function cancel($signal = 9) {
		if (!$this->proc) {
			return;
		}

		$this->kill($signal);
		\Amp\cancel($this->stdout);
		\Amp\cancel($this->stderr);
		\Amp\cancel($this->stdin);
		$this->resolve(new \RuntimeException("Process watching was cancelled"));

		foreach ($this->writeDeferreds as $deferred) {
			$deferred->fail(new \Exception("Write could not be completed, process watching was cancelled"));
		}
		$this->writeDeferreds = [];
	}

	public function pid() {
		if (!$this->proc) {
			return;
		}

		return \proc_get_status($this->proc)["pid"];
	}

	/**
	 * @return Promise which will succeed after $str was written. It will contain the total number of already written bytes to the process
	 */
	public function write($str) {
		assert(strlen($str) > 0);

		if (!$this->proc) {
			throw new \RuntimeException("Process already terminated");
		}

		$this->writeBuf .= $str;
		\Amp\enable($this->stdin);

		$this->writeTotal += strlen($str);
		$deferred = $this->writeDeferreds[$this->writeTotal] = new Deferred;

		return $deferred->promise();
	}
}
