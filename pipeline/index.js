const axios = require('axios');
const cheerio = require('cheerio');
const pLimit = require('p-limit');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

/**
 * 통합 데이터 파이프라인 (GitHub Actions용)
 * 1. 랭킹 페이지 스크래핑 (6차 승급 이상)
 * 2. 넥슨 API 호출 (Rate Limit 준수)
 * 3. Supabase Upsert
 */

const NEXON_API_KEY = process.env.NEXON_API_KEY;
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY);
const limit = pLimit(250); // 안전을 위해 초당 250회로 제한

const SERVERS = { '연': 1, '무휼': 2, '유리': 3, '하자': 4, '호동': 5, '진': 6 };
const SERVER_CODES = { '연': 131073, '무휼': 131074, '유리': 131086, '하자': 131087, '호동': 131088, '진': 131089 };
const JOBS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

async function runPipeline() {
  console.log('>>> 파이프라인 시작:', new Date().toISOString());

  for (const [serverName, serverId] of Object.entries(SERVERS)) {
    for (const jobCode of JOBS) {
      console.log(`[*] 수집 중: ${serverName} (직업 코드: ${jobCode})`);
      const characters = await scrapeRanking(serverName, jobCode);
      
      const tasks = characters.map(char => limit(() => processCharacter(char, serverName, serverId)));
      await Promise.all(tasks);
    }
  }

  console.log('>>> 파이프라인 완료:', new Date().toISOString());
}

async function scrapeRanking(serverName, jobCode) {
  const users = [];
  const serverCode = SERVER_CODES[serverName];
  let page = 0;

  while (page < 1) { // 상위 1000명 위주
    const startRank = (page * 20) + 1;
    const url = `https://baram.nexon.com/Rank/List?maskGameCode=${serverCode}&n4Rank_start=${startRank}&codeGameJob=${jobCode}`;
    
    try {
      const resp = await axios.get(url, { timeout: 10000 });
      const $ = cheerio.load(resp.data);
      const rows = $('.border_rank_list table tbody tr:not(.no_data)');

      if (rows.length === 0) break;

      let hasSixPromotion = false;
      rows.each((_, el) => {
        const name = $(el).find('td').eq(2).text().trim();
        const promoText = $(el).find('td').eq(5).text().trim();
        const promo = parseInt(promoText) || 0;

        if (promo >= 6) {
          users.push({ name, promo });
          hasSixPromotion = true;
        }
      });

      if (!hasSixPromotion) break; // 6차 미만 영역 진입 시 중단
      page++;
    } catch (err) {
      console.error(`Scrape Error (${serverName}):`, err.message);
      break;
    }
  }
  return users;
}

async function processCharacter(char, serverName, serverId) {
  try {
    // 1. OCID 조회
    const idResp = await axios.get(`https://open.api.nexon.com/baram/v1/id?character_name=${encodeURIComponent(char.name)}&server_name=${encodeURIComponent(serverName)}`, {
      headers: { 'x-nxopen-api-key': NEXON_API_KEY }
    });
    const { ocid } = idResp.data;

    // 2. 기본 정보 (레벨)
    const basicResp = await axios.get(`https://open.api.nexon.com/baram/v1/character/basic?ocid=${ocid}`, {
      headers: { 'x-nxopen-api-key': NEXON_API_KEY }
    });
    const level = basicResp.data.character_level;

    // 3. 장비 정보
    const equipResp = await axios.get(`https://open.api.nexon.com/baram/v1/character/item-equipment?ocid=${ocid}`, {
      headers: { 'x-nxopen-api-key': NEXON_API_KEY }
    });
    
    const equipment = equipResp.data.item_equipment || [];
    const itemIds = [];

    for (const item of equipment) {
      if (!item.item_name) continue;
      const itemId = generateItemId(item.item_name, item.item_equipment_slot_name);
      
      await supabase.from('items').upsert({
        item_id: itemId,
        name: item.item_name,
        part: item.item_equipment_slot_name
      });
      itemIds.push(itemId);
    }

    // 4. DB Upsert
    await supabase.from('users').upsert({
      server_id: serverId,
      character_name: char.name,
      level: level,
      equipment_ids: itemIds
    }, { onConflict: 'server_id, character_name' });

    process.stdout.write('.');
  } catch (err) {
    // 404 등 캐릭터 정보가 없는 경우 무시
  }
}

function generateItemId(name, part) {
  let hash = 0;
  const str = name + part;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return (Math.abs(hash) % 30000) + 1;
}

runPipeline().catch(console.error);
