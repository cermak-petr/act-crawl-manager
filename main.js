const Apify = require('apify');
const request = require('request-promise');

let input, state;
const output = {executionIds: []};

async function saveState(newState){
    for(var key in newState){
        state[key] = newState[key];
    }
    await Apify.setValue('STATE', state);
}

async function isCrawlerRunning(crawlerId){
    const exec = await Apify.client.crawlers.getLastExecution({crawlerId: crawlerId});
    return exec.status === 'RUNNING';
}

function waitForCrawlerFinish(crawlerId){
    return new Promise((resolve, reject) => {
        const interval = setInterval(async function(){
            const exec = await Apify.client.crawlers.getLastExecution({crawlerId: crawlerId});
            if(exec.status != 'RUNNING'){
                clearInterval(interval);
                resolve(exec);
            }
        }, 1000);
    });
}

function runActions(actions, parallels){
    return new Promise((resolve, reject) => {
        let toRun = 0;
        let running = 0;
        const done = state.done || Array(actions.length).fill(false);
        const getNext = () => _.findIndex(done, (e) => e === false);
        const results = [];
        const runNext = () => {
            const current = getNext();
            if(current > -1){
                running++;
                done[current] = null;
                actions[current]().then(async (result) => {
                    running--;
                    done[current] = true;
                    results.push(result);
                    await saveState({done: done.map((val) => val ? true : false)});
                    if(getNext() > -1 && running < parallels){
                        runNext();
                    }
                    else if(running === 0){resolve(results);}
                });
            }
        }
        _.each(actions.slice(0, Math.min(parallels, actions.length)), runNext);
    });
}

function createCrawlerActions(crawlers){
    const actions = [];
    _.each(crawlers, (crawler) => {
        actions.push(async () => {
            if(!(await isCrawlerRunning(crawler.id))){
                console.log('starting crawler: ' + crawler.id);
                await Apify.client.crawlers.startExecution({
                    crawlerId: crawler.id, 
                    settings: crawler.settings
                });
            }
            else{console.log('waiting for crawler: ' + crawler.id);}
            const run = await waitForCrawlerFinish(crawler.id);
            output.executionIds.push(run._id);
            console.log('crawler finished: ' + crawler.id);
        });
    });
    return actions;
}

Apify.main(async () => {
    input = await Apify.getValue('INPUT');
    state = (await Apify.getValue('STATE')) || {};
    
    if(!input.crawlers){return console.log('missing "crawlers" attribute in INPUT');}
    if(!input.parallel){input.parallel = 5;}
    
    const actions = createCrawlerActions(input.crawlers);
    await runActions(actions, input.parallel);
    
    Apify.setValue('OUTPUT', output);
});
