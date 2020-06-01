const _ = require('lodash');

const DURATION = {
  ms: (ms) => ms,
  seconds: (ms) => ms * 1000,
  minutes: (ms) => ms * 60 * 1000,
  hours: (ms) => ms * 60 * 60 * 1000,
};

const getRangeGenerator = (min, max, rand = Math.random) => () =>
  Math.trunc(rand() * (max - min)) + min;

class DelayGenerator {
  constructor(options) {
    const { delayMs } = options;
    let getDelayMs = getRangeGenerator(200, 1000);
    if (_.isNumber(delayMs)) {
      getDelayMs = () => delayMs;
    } else if (_.isArray(delayMs) && delayMs.length >= 2) {
      getDelayMs = getRangeGenerator(...delayMs);
    } else if (_.isPlainObject(delayMs)) {
      const { min, max, rand, strategy = 'DEFAULT' } = delayMs;
      switch (strategy) {
        case 'STICKY': {
          this._until = 0;
          const that = this;
          const ranges = delayMs.ranges;
          const untilGen = getRangeGenerator(min, max, rand);
          getDelayMs = () => {
            if (+new Date() > that._until) {
              const range = ranges[Math.trunc(Math.random() * ranges.length)];
              const { min, max, rand, key } = range;
              that._rangeGen = getRangeGenerator(min, max, rand);
              const duration = untilGen();
              that._until = +new Date() + duration;
              console.log(
                `\tModifying sticky schedule [${key}] min=${min} max=${max} duration=${duration} until="${new Date(
                  that._until,
                ).toISOString()}"`,
              );
            }
            return that._rangeGen();
          };
          break;
        }
        default: {
          getDelayMs = getRangeGenerator(min, max, rand);
        }
      }
    }
    this.get = getDelayMs;
  }
}

DelayGenerator.getRangeGenerator = getRangeGenerator;
DelayGenerator.DURATION = DURATION;

module.exports = DelayGenerator;
