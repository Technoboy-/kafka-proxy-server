package com.owl.kafka.proxy.server.biz.bo;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class ManyPullRequest {

    private final ArrayList<PullRequest> requestHolder = new ArrayList<>();

    public synchronized void add(final PullRequest pullRequest) {
        this.requestHolder.add(pullRequest);
    }

    public synchronized void add(final List<PullRequest> many) {
        this.requestHolder.addAll(many);
    }

    public synchronized List<PullRequest> cloneAndClear() {
        if (!this.requestHolder.isEmpty()) {
            List<PullRequest> result = (ArrayList<PullRequest>) this.requestHolder.clone();
            this.requestHolder.clear();
            return result;
        }

        return null;
    }
}
