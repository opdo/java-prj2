package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WebCrawlerTask extends RecursiveAction {
    private final Clock clock;
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final List<Pattern> ignoredUrls;
    private final ConcurrentSkipListSet<String> visitedUrls;
    private final ConcurrentHashMap<String, Integer> counts;
    private final PageParserFactory parserFactory;

    public WebCrawlerTask(Clock clock, String url, Instant deadline, int maxDepth, List<Pattern> ignoredUrls, ConcurrentSkipListSet<String> visitedUrls, ConcurrentHashMap<String, Integer> counts, PageParserFactory parserFactory) {
        this.url = url;
        this.clock = clock;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.ignoredUrls = ignoredUrls;
        this.visitedUrls = visitedUrls;
        this.counts = counts;
        this.parserFactory = parserFactory;
    }

    @Override
    protected void compute() {
        // End task
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return;
        }

        // Ignore urls
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return;
            }
        }

        synchronized (this) {
            // Skip visited urls
            if (visitedUrls.contains(url)) {
                return;
            }

            // Add to list visited urls
            visitedUrls.add(url);
        }

        // Crawler data
        PageParser.Result result = parserFactory.get(url).parse();

        synchronized (this) {
            for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
                counts.compute(e.getKey(), (k, v) -> v == null ? e.getValue() : v + e.getValue());
            }
        }

        // Loop
        List<WebCrawlerTask> subtasks = result.getLinks().stream()
                .map(childUrl -> new WebCrawlerTask(clock, childUrl, deadline, maxDepth - 1, ignoredUrls, visitedUrls, counts, parserFactory))
                .collect(Collectors.toList());
        invokeAll(subtasks);
    }
}
