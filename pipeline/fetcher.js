import pLimit from 'p-limit';
import { createClient } from '@supabase/supabase-js';

/**
 * Fetcher Worker (Cloudflare Queue Consumer)
 * - Queue에서 캐릭터명을 받아 넥슨 API를 호출하고 Supabase에 저장합니다.
 * - p-limit을 사용하여 초당 API 호출 수를 300회로 제한합니다.
 */

const limit = pLimit(300); // 넥슨 API 초당 300회 제한

export default {
  async queue(batch, env) {
    const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_ANON_KEY);
    const NEXON_API_KEY = env.NEXON_API_KEY;

    const tasks = batch.messages.map(msg =>
      limit(async () => {
        const { serverName, characterName } = msg.body;

        try {
          // 1. Get OCID
          const idResponse = await fetch(`https://open.api.nexon.com/baram/v1/id?character_name=${encodeURIComponent(characterName)}&server_name=${encodeURIComponent(serverName)}`, {
            headers: { 'x-nxopen-api-key': NEXON_API_KEY }
          });
          if (!idResponse.ok) return;
          const { ocid } = await idResponse.json();

          // 2. Get Basic Info (Level)
          const basicResponse = await fetch(`https://open.api.nexon.com/baram/v1/character/basic?ocid=${ocid}`, {
            headers: { 'x-nxopen-api-key': NEXON_API_KEY }
          });
          const basicData = await basicResponse.json();
          const level = basicData.character_level;

          // 3. Get Equipment Info
          const equipResponse = await fetch(`https://open.api.nexon.com/baram/v1/character/item-equipment?ocid=${ocid}`, {
            headers: { 'x-nxopen-api-key': NEXON_API_KEY }
          });
          const equipData = await equipResponse.json();

          // 장비 데이터 추출 및 items 테이블용 정규화
          const equipmentList = equipData.item_equipment || [];
          const itemIds = [];

          for (const item of equipmentList) {
            if (!item.item_name) continue;

            // 넥슨 API에서 아이템 고유 ID가 제공되지 않을 경우, 이름+부위로 해싱하거나 
            // 별도의 매핑 테이블을 운영해야 하지만 여기선 API에서 제공하는 고유 ID가 있다고 가정 (또는 생성)
            // 실제 바람 API는 item_equipment_slot_name 등으로 구분됨
            const mockItemId = generateItemId(item.item_name, item.item_equipment_slot_name);

            // Item Upsert
            await supabase.from('items').upsert({
              item_id: mockItemId,
              name: item.item_name,
              part: item.item_equipment_slot_name
            }, { onConflict: 'item_id' });

            itemIds.push(mockItemId);
          }

          // 4. User Upsert (Unique constraint: server_id, character_name)
          const serverId = getServerId(serverName);
          await supabase.from('users').upsert({
            server_id: serverId,
            character_name: characterName,
            level: level,
            equipment_ids: itemIds
          }, { onConflict: 'server_id, character_name' });

          // 성공적으로 처리된 메시지 확인
          msg.ack();
        } catch (err) {
          console.error(`Error processing ${characterName}:`, err);
          // 실패 시 재시도하도록 놔둠 (nack)
        }
      })
    );

    await Promise.all(tasks);
  }
};

// 서버명 -> 매핑 ID (1~6)
function getServerId(name) {
  const mapping = { '연': 1, '무휼': 2, '유리': 3, '하자': 4, '호동': 5, '진': 6 };
  return mapping[name] || 0;
}

// 아이템 고유 ID 생성을 위한 간단한 해시 함수 (최대 30,000개 제한 고려)
function generateItemId(name, part) {
  let hash = 0;
  const str = name + part;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  // SMALLINT(32767) 범위 내로 조정
  return (Math.abs(hash) % 30000) + 1;
}
