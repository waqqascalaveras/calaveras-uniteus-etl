"""
================================================================================
Calaveras UniteUs ETL - Test Orchestrator
================================================================================
Developed by Waqqas Hanafi
Calaveras County Health and Human Services Agency

Description:
    Test orchestrator that runs tests with timeout protection,
    output capture, and proper logging. Prevents tests from hanging
    indefinitely and provides detailed test execution reports.

Features:
    - Timeout protection (kills hanging tests)
    - Output capture and logging
    - Test result aggregation
    - Detailed reporting
    - Configurable timeout per test
    - Works with pytest
================================================================================
"""

import subprocess
import sys
import os
import time
from pathlib import Path
from typing import List, Dict, Optional
import json

# Import signal only on Unix
if sys.platform != 'win32':
    import signal
else:
    signal = None


class TestOrchestrator:
    """Orchestrates test execution with timeout protection"""
    
    def __init__(self, 
                 test_timeout: int = 30,
                 log_dir: Optional[Path] = None,
                 verbose: bool = True):
        """
        Initialize test orchestrator
        
        Args:
            test_timeout: Maximum seconds per test before killing (default: 30)
            log_dir: Directory for test logs (default: tests/logs)
            verbose: Enable verbose output
        """
        self.test_timeout = test_timeout
        self.log_dir = log_dir or Path(__file__).parent / "logs"
        self.verbose = verbose
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.results: List[Dict] = []
        self.start_time = None
        self.end_time = None
        
    def run_test_with_timeout(self, 
                              test_path: str,
                              test_name: Optional[str] = None) -> Dict:
        """
        Run a single test with timeout protection
        
        Args:
            test_path: Path to test file or test class/method
            test_name: Optional test name for logging
            
        Returns:
            Dictionary with test results
        """
        if test_name is None:
            test_name = test_path
        
        # Sanitize filename
        safe_name = test_name.replace(':', '_').replace('.', '_').replace('\\', '_').replace('/', '_')
        log_file = self.log_dir / f"{safe_name}_{int(time.time())}.log"
        
        result = {
            "test_name": test_name,
            "test_path": test_path,
            "status": "unknown",
            "duration": 0,
            "output": "",
            "error": "",
            "log_file": str(log_file),
            "timed_out": False
        }
        
        start_time = time.time()
        process = None
        
        try:
            # Build command - use absolute path to avoid issues
            project_root = Path(__file__).parent.parent.absolute()
            # Use pytest instead of unittest
            cmd = [sys.executable, "-m", "pytest", test_path, "-v", "--tb=short"]
            
            # Start process with explicit environment
            env = os.environ.copy()
            env['PYTHONUNBUFFERED'] = '1'  # Prevent buffering
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(project_root),
                env=env,
                bufsize=1  # Line buffered
            )
            
            # Wait with timeout
            try:
                stdout, stderr = process.communicate(timeout=self.test_timeout)
                return_code = process.returncode
                
                result["duration"] = time.time() - start_time
                result["output"] = stdout or ""
                result["error"] = stderr or ""
                
                if return_code == 0:
                    result["status"] = "passed"
                else:
                    result["status"] = "failed"
                    
            except subprocess.TimeoutExpired:
                # Kill the process immediately
                result["timed_out"] = True
                result["status"] = "timeout"
                result["duration"] = time.time() - start_time
                
                # Force kill immediately - no graceful termination
                try:
                    if sys.platform == 'win32':
                        # Windows: use taskkill for more reliable killing
                        subprocess.run(
                            ['taskkill', '/F', '/T', '/PID', str(process.pid)],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            timeout=2
                        )
                    else:
                        # Unix: kill process group
                        process.kill()
                        try:
                            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        except:
                            pass
                except Exception as e:
                    # If killing fails, just note it
                    result["error"] = f"[TEST TIMED OUT - KILL ATTEMPT: {e}]"
                else:
                    result["error"] = "[TEST TIMED OUT AND WAS KILLED]"
                
                # Try to get any partial output
                try:
                    process.wait(timeout=1)
                except:
                    pass
                
        except Exception as e:
            result["status"] = "error"
            result["error"] = f"Error running test: {str(e)}"
            result["duration"] = time.time() - start_time
            # Make sure process is dead
            if process:
                try:
                    process.kill()
                except:
                    pass
        
        # Write log file
        try:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"Test: {test_name}\n")
                f.write(f"Path: {test_path}\n")
                f.write(f"Status: {result['status']}\n")
                f.write(f"Duration: {result['duration']:.2f}s\n")
                f.write(f"Timed Out: {result['timed_out']}\n")
                f.write(f"\n{'='*70}\n")
                f.write("STDOUT:\n")
                f.write(f"{'='*70}\n")
                f.write(result['output'])
                f.write(f"\n{'='*70}\n")
                f.write("STDERR:\n")
                f.write(f"{'='*70}\n")
                f.write(result['error'])
        except Exception as e:
            result["error"] += f"\n[ERROR WRITING LOG: {e}]"
        
        return result
    
    def discover_tests(self, test_dir: Optional[Path] = None) -> List[str]:
        """
        Discover all test files in the test directory
        
        Args:
            test_dir: Directory to search (default: tests/)
            
        Returns:
            List of test paths
        """
        if test_dir is None:
            test_dir = Path(__file__).parent
        
        test_files = []
        project_root = Path(__file__).parent.parent
        
        # Find all test_*.py files recursively
        try:
            for test_file in test_dir.rglob("test_*.py"):
                # Skip the orchestrator itself
                if test_file.name == "test_orchestrator.py":
                    continue
                # Add file-level test - use relative path from project root
                rel_path = test_file.relative_to(project_root)
                # Use pytest path format
                test_files.append(str(rel_path).replace('\\', '/').replace('.py', ''))
        except Exception as e:
            print(f"Error discovering tests: {e}")
        
        return sorted(test_files)
    
    def run_all_tests(self, 
                     test_files: Optional[List[str]] = None,
                     max_failures: Optional[int] = None) -> Dict:
        """
        Run all tests with timeout protection
        
        Args:
            test_files: List of test files to run (None = discover)
            max_failures: Stop after N failures (None = run all)
            
        Returns:
            Summary dictionary
        """
        if test_files is None:
            test_files = self.discover_tests()
        
        self.start_time = time.time()
        
        print(f"\n{'='*70}")
        print(f"TEST ORCHESTRATOR")
        print(f"{'='*70}")
        print(f"Timeout per test: {self.test_timeout}s")
        print(f"Test files: {len(test_files)}")
        print(f"Log directory: {self.log_dir}")
        print(f"{'='*70}\n")
        
        passed = 0
        failed = 0
        timed_out = 0
        errors = 0
        
        for i, test_file in enumerate(test_files, 1):
            print(f"[{i}/{len(test_files)}] Running: {test_file}", flush=True)
            
            try:
                result = self.run_test_with_timeout(test_file)
                self.results.append(result)
            except KeyboardInterrupt:
                print("\n⚠ Test execution interrupted by user")
                break
            except Exception as e:
                print(f"  ! ERROR running test: {e}")
                result = {
                    "test_name": test_file,
                    "test_path": test_file,
                    "status": "error",
                    "duration": 0,
                    "output": "",
                    "error": str(e),
                    "log_file": "",
                    "timed_out": False
                }
                self.results.append(result)
            
            # Print result
            status_symbol = {
                "passed": "✓",
                "failed": "✗",
                "timeout": "⏱",
                "error": "!"
            }.get(result["status"], "?")
            
            duration_str = f"{result['duration']:.2f}s"
            if result["timed_out"]:
                duration_str = f"{duration_str} (TIMEOUT)"
            
            print(f"  {status_symbol} {result['status'].upper():8} {duration_str}")
            
            if result["status"] == "passed":
                passed += 1
            elif result["status"] == "timeout":
                timed_out += 1
                failed += 1
            elif result["status"] == "failed":
                failed += 1
            else:
                errors += 1
                failed += 1
            
            # Stop if max failures reached
            if max_failures and failed >= max_failures:
                print(f"\n⚠ Stopping after {failed} failures (max_failures={max_failures})")
                break
        
        self.end_time = time.time()
        total_duration = self.end_time - self.start_time
        
        # Print summary
        print(f"\n{'='*70}")
        print(f"TEST SUMMARY")
        print(f"{'='*70}")
        print(f"Total tests: {len(self.results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"  - Timeouts: {timed_out}")
        print(f"  - Errors: {errors}")
        print(f"Total duration: {total_duration:.2f}s")
        print(f"{'='*70}\n")
        
        # Print failed tests
        failed_tests = [r for r in self.results if r["status"] != "passed"]
        if failed_tests:
            print(f"FAILED TESTS ({len(failed_tests)}):")
            print(f"{'='*70}")
            for result in failed_tests:
                print(f"  {result['status'].upper()}: {result['test_name']}")
                if result["timed_out"]:
                    print(f"    ⏱ Timed out after {result['duration']:.2f}s")
                if result["error"]:
                    error_lines = result["error"].split('\n')[:3]
                    for line in error_lines:
                        if line.strip():
                            print(f"    {line[:100]}")
                print(f"    Log: {result['log_file']}")
            print()
        
        summary = {
            "total": len(self.results),
            "passed": passed,
            "failed": failed,
            "timed_out": timed_out,
            "errors": errors,
            "duration": total_duration,
            "results": self.results
        }
        
        # Save summary to JSON
        summary_file = self.log_dir / f"test_summary_{int(time.time())}.json"
        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"Summary saved to: {summary_file}")
        except Exception as e:
            print(f"Warning: Could not save summary: {e}")
        
        return summary


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Orchestrator with Timeout Protection")
    parser.add_argument("--timeout", type=int, default=30,
                       help="Timeout per test in seconds (default: 30)")
    parser.add_argument("--log-dir", type=str, default=None,
                       help="Log directory (default: tests/logs)")
    parser.add_argument("--test", type=str, action="append",
                       help="Specific test to run (can be specified multiple times)")
    parser.add_argument("--max-failures", type=int, default=None,
                       help="Stop after N failures")
    parser.add_argument("--quiet", action="store_true",
                       help="Disable verbose output")
    
    args = parser.parse_args()
    
    log_dir = Path(args.log_dir) if args.log_dir else None
    
    orchestrator = TestOrchestrator(
        test_timeout=args.timeout,
        log_dir=log_dir,
        verbose=not args.quiet
    )
    
    if args.test:
        test_files = args.test
    else:
        test_files = None
    
    summary = orchestrator.run_all_tests(
        test_files=test_files,
        max_failures=args.max_failures
    )
    
    # Exit with appropriate code
    sys.exit(0 if summary["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
