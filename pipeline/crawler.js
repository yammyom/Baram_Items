/**
 * Crawler Worker (Cloudflare Worker)
 * - Cron Trigger로 실행되어 홈페이지 랭킹을 스크래핑합니다.
 * - 6차 승급 이상의 유저만 추출하여 Queue로 전달합니다.
 */

export default {
  async scheduled(event, env, ctx) {
    const SERVERS = {
      '연': 131073, '무휼': 131074, '유리': 131086,
      '하자': 131087, '호동': 131088, '진': 131089
    };

    const JOBS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]; // 모든 직업
    const TARGET_PROMOTION = 6; // 6차 승급 이상

    for (const [serverName, serverCode] of Object.entries(SERVERS)) {
      for (const jobCode of JOBS) {
        let page = 0;
        let shouldContinue = true;

        while (shouldContinue && page < 50) { // 과도한 스크래핑 방지 (상위권 위주)
          const startRank = (page * 20) + 1;
          const url = `https://baram.nexon.com/Rank/List?maskGameCode=${serverCode}&n4Rank_start=${startRank}&codeGameJob=${jobCode}`;

          try {
            const response = await fetch(url);
            const html = await response.text();

            // Note: Cloudflare Workers에서는 외부 라이브러리(cheerio) 없이 정규표현식이나 HTMLRewriter를 사용하거나,
            // npm install cheerio 후 wrangler로 번들링해야 합니다. 여기서는 로직 중심의 의사코드를 포함합니다.
            const characters = parseRankingPage(html); // 스크래핑 함수 (아래 구현 참고)

            if (characters.length === 0) break;

            const targetUsers = characters.filter(u => u.promotion >= TARGET_PROMOTION);

            for (const user of targetUsers) {
              await env.BARAM_QUEUE.send({
                serverName,
                serverCode,
                characterName: user.name
              });
            }

            // 만약 현재 페이지의 마지막 유저가 6차 미만이면 해당 직업/서버 탐색 중단
            if (characters[characters.length - 1].promotion < TARGET_PROMOTION) {
              shouldContinue = false;
            }

            page++;
            // Rate Limit 방지를 위해 짧은 대기
            await new Promise(r => setTimeout(r, 500));
          } catch (err) {
            console.error(`Error scraping ${serverName} Job ${jobCode}:`, err);
            break;
          }
        }
      }
    }
  }
};

/**
 * HTML 파싱 로직 (의사코드 형식)
 */
function parseRankingPage(html) {
  const results = [];
  // 실제 구현시 cheerio 사용 권장 (npm install cheerio)
  // 예시: const $ = cheerio.load(html); 
  // $('tr').each(...)

  // 여기서는 간단한 정규식으로 예시 구현
  const rowRegex = /<tr>.*?<td>(\d+)<\/td>.*?<td>.*?<\/td>.*?<td>(.*?)<\/td>.*?<td>.*?<\/td>.*?<td>.*?<\/td>.*?<td>(\d+)차<\/td>.*?<\/tr>/gs;
  let match;
  while ((match = rowRegex.exec(html)) !== null) {
    results.push({
      rank: parseInt(match[1]),
      name: match[2].trim(),
      promotion: parseInt(match[3])
    });
  }
  return results;
}
