/*
	This file is part of jLCM - see https://github.com/martinkirch/jlcm/
	
	Copyright 2013,2014 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Joseph Fourier and CNRS

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/


package fr.liglab.jlcm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.jlcm.internals.ExplorationStep;
import fr.liglab.jlcm.io.AllFISConverter;
import fr.liglab.jlcm.io.MultiThreadedFileCollector;
import fr.liglab.jlcm.io.NullCollector;
import fr.liglab.jlcm.io.PatternSortCollector;
import fr.liglab.jlcm.io.PatternsCollector;
import fr.liglab.jlcm.io.PatternsWriter;
import fr.liglab.jlcm.io.StdOutCollector;
import fr.liglab.jlcm.util.MemoryPeakWatcherThread;

/**
 * jLCM as a command-line utility. Invoke without arguments to print the manual.
 */
public class RunPLCM {
	protected static long chrono;
	
	public static void main(String[] args) throws Exception {

		Options options = new Options();
		CommandLineParser parser = new PosixParser();
		
		options.addOption("a", false, "Output all frequent itemsets, not only closed ones");
		options.addOption(
				"b",
				false,
				"Benchmark mode : patterns are not outputted at all (in which case OUTPUT_PATH is ignored)");
		options.addOption("h", false, "Show help");
		options.addOption(
				"m",
				false,
				"Give peak memory usage after mining (instanciates a watcher thread that periodically triggers garbage collection)");
		options.addOption("s", false, "Sort items in outputted patterns, in ascending order");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");
		options.addOption("v", false, "Enable verbose mode, which logs every extension of the empty pattern");
		options.addOption("V", false,
				"Enable ultra-verbose mode, which logs every pattern extension (use with care: it may produce a LOT of output)");

		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.getArgs().length < 2 || cmd.getArgs().length > 3 || cmd.hasOption('h')) {
				printMan(options);
			} else {
				standalone(cmd);
			}
		} catch (ParseException e) {
			printMan(options);
		}
	}

	public static void printMan(Options options) {
		String syntax = "java fr.liglab.mining.RunPLCM [OPTIONS] INPUT_PATH MINSUP [OUTPUT_PATH]";
		String header = "\nIf OUTPUT_PATH is missing, patterns are printed to standard output.\nOptions are :";
		String footer = "Copyright 2013,2014 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, "
				+ "Sihem Amer-Yahia, Marie-Christine Rousset, Université Joseph Fourier and CNRS";

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80, syntax, header, options, footer);
	}

	public static void standalone(CommandLine cmd) {
		String[] args = cmd.getArgs();
		int minsup = Integer.parseInt(args[1]);
		MemoryPeakWatcherThread memoryWatch = null;

		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}
		
		if (cmd.hasOption('m')) {
			memoryWatch = new MemoryPeakWatcherThread();
			memoryWatch.start();
		}

		chrono = System.currentTimeMillis();
		ExplorationStep initState = new ExplorationStep(minsup, args[0]);
		long loadingTime = System.currentTimeMillis() - chrono;
		System.err.println("Dataset loaded in " + loadingTime + "ms");

		if (cmd.hasOption('V')) {
			ExplorationStep.verbose = true;
			ExplorationStep.ultraVerbose = true;
		} else if (cmd.hasOption('v')) {
			ExplorationStep.verbose = true;
		}

		int nbThreads = Runtime.getRuntime().availableProcessors();
		if (cmd.hasOption('t')) {
			nbThreads = Integer.parseInt(cmd.getOptionValue('t'));
		}

		PatternsCollector collector = instanciateCollector(cmd, outputPath, nbThreads);

		PLCM miner = new PLCM(collector, nbThreads);

		chrono = System.currentTimeMillis();
		miner.lcm(initState);
		chrono = System.currentTimeMillis() - chrono;

		Map<String, Long> additionalCounters = new HashMap<String, Long>();
		additionalCounters.put("miningTime", chrono);
		additionalCounters.put("outputtedPatterns", collector.close());
		additionalCounters.put("loadingTime", loadingTime);
		additionalCounters.put("avgPatternLength", (long) collector.getAveragePatternLength());

		if (memoryWatch != null) {
			memoryWatch.interrupt();
			additionalCounters.put("maxUsedMemory", memoryWatch.getMaxUsedMemory());
		}

		System.err.println(miner.toString(additionalCounters));
	}

	/**
	 * Parse command-line arguments to instantiate the right collector
	 * 
	 * @param nbThreads
	 */
	private static PatternsCollector instanciateCollector(CommandLine cmd, String outputPath,
			int nbThreads) {

		PatternsCollector collector = null;

		if (cmd.hasOption('b')) {
			collector = new NullCollector();
		} else {
			PatternsWriter writer = null;
			
			if (outputPath != null) {
				try {
					writer = new MultiThreadedFileCollector(outputPath, nbThreads);
				} catch (IOException e) {
					e.printStackTrace(System.err);
					System.err.println("Aborting mining.");
					System.exit(1);
				}
			} else {
				writer = new StdOutCollector();
			}
			
			collector = writer;
			
			if (cmd.hasOption('a')) {
				collector = new AllFISConverter(writer);
			}

			if (cmd.hasOption('s')) {
				collector = new PatternSortCollector(collector);
			}
		}

		return collector;
	}

}
