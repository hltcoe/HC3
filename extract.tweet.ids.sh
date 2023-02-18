#!/bin/bash
zcat "$@" | jq -cr '.tweet_ids[] | .[1]'