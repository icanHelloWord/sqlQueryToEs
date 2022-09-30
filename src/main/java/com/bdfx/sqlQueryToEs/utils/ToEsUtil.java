package com.bdfx.sqlQueryToEs.utils;

import com.alibaba.fastjson.JSON;
import com.bdfx.sqlQueryToEs.constant.MysqlKeyword;
import com.bdfx.sqlQueryToEs.constant.Symbols;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Slf4j
public class ToEsUtil {

    public static void main(String[] args) {
        //测试
        String condition = "a = 1 AND b > 2 OR c < 3 or     d >= 4 AND  e <= 5 AND  f BETWEEN 1 AND 2 AND  g IN () or  h NOT IN() and i IS NULL " +
                "OR j IS   NOT     NULL GROUP BY a,b ORDER BY a,b DESC,c,d asc LIMIT 1,10";
        String condition1 = "field1 = 1 AND (field2 =3 or field3 < 3)   AND   field4 >= 4 AND  field5 between 1 and 10 and field6 IS NULL and field7 IS   NOT     NULL  and field9 like '%我%' and field10 in(1,2,3) GROUP BY field1,field2 ORDER BY field3,field4 DESC,field5,field6 asc LIMIT 1,10";

        String condition2 = "bookAuthor LIKE '%老%' AND bookId IN ( 101 , 102) GROUP BY bookId HAVING num>1";

        buildEsQueryConditionByMysqlCondition(condition);
    }


    /**
     *mysql条件转换成es条件格式
     */
    public static Map<String,Object> buildEsQueryConditionByMysqlCondition(String condition){
        if (StringUtils.isBlank(condition)) {
            throw new RuntimeException(" condition is null or empty ");
        }
        if (isConditionHashFunction(condition)) {
            throw new RuntimeException(" condition not support mysql function ");
        }
        log.info("原始的SQL条件内容：{}",condition);
        String newCondition = formatCondition(condition);
        log.info("格式化后的SQL条件：{}",newCondition);
        Map<String,Object> esQuery = new HashMap<>();
        if (StringUtils.countMatches(newCondition,MysqlKeyword.SELECT) > 1){
            throw new RuntimeException(" condition not support sub select ");
        }
        String afterDisassemblyCondition = newCondition;
        //处理 limit 内容
        if (afterDisassemblyCondition.contains(MysqlKeyword.LIMIT)){
            String[] limit = afterDisassemblyCondition.split(MysqlKeyword.LIMIT);
            String limitValues = limit[1];
            buildLimit(esQuery,limitValues);
            afterDisassemblyCondition = limit[0];

        }
        //处理 order by 内容
        if (afterDisassemblyCondition.contains(MysqlKeyword.ORDER_BY)){
            String[] orderBy = afterDisassemblyCondition.split(MysqlKeyword.ORDER_BY);
            String orderByValues = orderBy[1];
            List<Map<String,Object>> sort = buildSort(orderByValues);
            esQuery.put("sort",sort);
            afterDisassemblyCondition = orderBy[0];
        }
        //处理 group by 内容
        if (afterDisassemblyCondition.contains(MysqlKeyword.GROUP_BY)){
            String[] groupBy = afterDisassemblyCondition.split(MysqlKeyword.GROUP_BY);
            String groupByValues = groupBy[1];
            Map<String,Object> aggregations = buildGroup(groupByValues);
            esQuery.put("aggregations",aggregations);
            afterDisassemblyCondition = groupBy[0];
        }

        //处理 where 后条件内容
        Map<String, Object> query = buildWhere(afterDisassemblyCondition);
        esQuery.put("query",query);
        log.info("转成ES结果：{}",JSON.toJSONString(esQuery));
        return esQuery;
    }



    //处理例如此类where条件 a = 1 AND b > 2 OR c < 3 or (d >= 4 AND  e <= 5) AND  f BETWEEN 1 AND 2 AND  g IN () or  h NOT IN() and i IS NULL OR j IS NOT NULL
    private static  Map<String, Object> buildWhere(String condition) {
        Map<String,Object> query = new HashMap<>();
        Map<String,Object> bool = new HashMap<>();

        // 没有出现过（）的查询，排除in，not in的括号
        if (!condition.contains(Symbols.LEFT_BRACKETS) ||
                (StringUtils.countMatches(condition,Symbols.LEFT_BRACKETS)
                        == StringUtils.countMatches(condition,MysqlKeyword.IN))) {
            if (condition.contains(MysqlKeyword.BETWEEN)){
                if (condition.contains(MysqlKeyword.OR)) {
                    //没有括号，有between，有or
                    dealNoBracketsWithOrAndBetween(condition, bool);
                }else {
                    //没有括号，有between，没有or
                    dealNoBracketsNoOrWithBetween(condition, bool);
                }
            } else {
                if (condition.contains(MysqlKeyword.OR)) {
                    //没有括号，没有between，有or
                    dealNoBracketsNoBetweenWithOr(condition, bool);
                }else {
                    //没有括号，没有between，没有or
                    dealNoBracketsNoBetweenNoOr(condition, bool);
                }
            }
        }else {
            //有括号的查询，最终都转成没有括号的查询
            dealWithBrackets(condition, bool);
        }
        query.put("bool", bool);
        return query;
    }

    private static void dealWithBrackets(String condition, Map<String, Object> bool) {
        //以括号开头
        if (Symbols.LEFT_BRACKETS.equals(String.valueOf(condition.charAt(0)))) {
            //寻找匹配的括号,找到之后把里面的内容和括号外的内容分开继续调buildWhere处理，括号后首次出现连接方式作为ES最外层的方式
            int leftBracketsNum = 1;
            int rightBracketsNum = 0;
            for (int index = 1 ; index < condition.length() ; index ++) {
                if (Symbols.LEFT_BRACKETS.equals(String.valueOf(condition.charAt(index)))){
                    leftBracketsNum++;
                }else if (Symbols.RIGHT_BRACKETS.equals(String.valueOf(condition.charAt(index)))) {
                    rightBracketsNum++;
                }
                if (leftBracketsNum == rightBracketsNum) {
                    List<Map<String,Object>> metas = new ArrayList<>();
                    String innerCondition = condition.substring(1,index);
                    metas.add(buildWhere(innerCondition.trim()));
                    String surplusCondition = condition.substring(index+1).trim();
                    if (surplusCondition.startsWith(MysqlKeyword.AND)) {
                        surplusCondition = surplusCondition.substring(MysqlKeyword.AND.length());
                        metas.add(buildWhere(surplusCondition.trim()));
                        bool.put("must",metas);
                    }else if (surplusCondition.startsWith(MysqlKeyword.OR)){
                        surplusCondition = surplusCondition.substring(MysqlKeyword.OR.length());
                        metas.add(buildWhere(surplusCondition.trim()));
                        bool.put("should",metas);
                    }else {
                        throw new RuntimeException("condition is wrong at "+index+1);
                    }
                    break;
                }
            }
//            }
        }else {
            //不是以括号开头的，则按照首次出现的连接方式确定ES连接方式,拆成两个独立的条件调buildWhere处理
            List<Map<String,Object>> metas = new ArrayList<>();
            if (condition.indexOf(MysqlKeyword.AND) < condition.indexOf(MysqlKeyword.OR)) {
                String firstCondition = condition.substring(0,condition.indexOf(MysqlKeyword.AND));
                metas.add(buildWhere(firstCondition.trim()));
                String secondCondition = condition.substring(condition.indexOf(MysqlKeyword.AND)+3);
                metas.add(buildWhere(secondCondition.trim()));
                bool.put("must",metas);
            }else {
                String firstCondition = condition.substring(0,condition.indexOf(MysqlKeyword.OR));
                metas.add(buildWhere(firstCondition.trim()));
                metas.add(buildWhere(condition.substring(condition.indexOf(MysqlKeyword.OR)+2)));
                bool.put("should",metas);
            }

        }
    }

    private static void dealNoBracketsNoBetweenWithOr(String condition, Map<String, Object> bool) {
        dealNoBracketsWithOrAndBetween(condition,bool);
    }


    private static void dealNoBracketsWithOrAndBetween(String condition, Map<String, Object> bool) {
        List<Map<String,Object>> metas = new ArrayList<>();
        String firstString = condition.substring(0,condition.lastIndexOf(MysqlKeyword.OR));
        String secondString = condition.substring(condition.lastIndexOf(MysqlKeyword.OR)+3);
        metas.add(buildWhere(firstString));
        metas.add(buildWhere(secondString));
        bool.put("should",metas);

    }

    private static void dealNoBracketsNoBetweenNoOr(String condition, Map<String, Object> bool) {
        String[] metaConditions = condition.split(MysqlKeyword.AND);
        List<Map<String,Object>> metas = new ArrayList<>();
        for (String metaCondition : metaConditions) {
            Map<String, Object> meta = buildMeta(metaCondition);
            if (meta.containsKey("must_not")) {
                Map<String,Object> mustNotBool = new HashMap<>();
                mustNotBool.put("bool",meta);
                metas.add(mustNotBool);
            }else {
                metas.add(meta);
            }
        }
        bool.put("must",metas);
    }

    private static void dealNoBracketsNoOrWithBetween(String condition, Map<String, Object> bool) {
        String[] metaConditions = condition.split(MysqlKeyword.AND);
        List<Map<String,Object>> metas = new ArrayList<>();
        for (int index =0;index < metaConditions.length;index++) {
            String metaCondition = metaConditions[index];
            if (metaCondition.contains(MysqlKeyword.BETWEEN)) {
                metaCondition = metaCondition+MysqlKeyword.AND+metaConditions[index+1];
                index++;
            }
            Map<String, Object> meta = buildMeta(metaCondition);
            if (meta.containsKey("must_not")) {
                Map<String,Object> mustNotBool = new HashMap<>();
                mustNotBool.put("bool",meta);
                metas.add(mustNotBool);
            }else {
                metas.add(meta);
            }
        }
        bool.put("must",metas);
    }

    //处理例如此类最小单元条件以ES格式返回 field1=1, field2 > 2 ,field in (1,2)等等
    private static Map<String, Object> buildMeta(String metaCondition) {
        Map<String,Object> meta = new HashMap<>();
        String[] values;
        if (metaCondition.contains(Symbols.EQ) && !metaCondition.contains(Symbols.LT) && !metaCondition.contains(Symbols.GT)&&
                !metaCondition.contains(Symbols.NEQ)) {
            values = metaCondition.split(Symbols.EQ);
            Map<String,Object> term = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("value",values[1].trim());
            term.put(values[0].trim(),value);
            meta.put("term",term);

        }else if (metaCondition.contains(Symbols.NEQ)|| metaCondition.contains(Symbols.NEQ2)){
            values = metaCondition.split(Symbols.NEQ);
            if (Objects.isNull(values) || values.length < 1) {
                values = metaCondition.split(Symbols.NEQ2);
            }
            List<Map<String,Object>> mustNot = new ArrayList<>();
            Map<String,Object> term = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("value",values[1].trim());
            term.put(values[0].trim(),value);
            mustNot.add(term);

            meta.put("must_not",mustNot);
        }else if (metaCondition.contains(Symbols.LT) && !metaCondition.contains(Symbols.LTE)){
            values = metaCondition.split(Symbols.LT);

            Map<String,Object> range = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("from",null);
            value.put("to",values[1].trim());
            value.put("include_lower",false);
            value.put("include_upper",false);
            range.put(values[0].trim(),value);
            meta.put("range",range);
        }else if (metaCondition.contains(Symbols.LTE)){
            values = metaCondition.split(Symbols.LTE);

            Map<String,Object> range = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("from",null);
            value.put("to",values[1].trim());
            value.put("include_lower",false);
            value.put("include_upper",true);
            range.put(values[0].trim(),value);
            meta.put("range",range);

        }else if (metaCondition.contains(Symbols.GT) && !metaCondition.contains(Symbols.GTE)){
            values = metaCondition.split(Symbols.GT);
            Map<String,Object> range = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("from",values[1].trim());
            value.put("to",null);
            value.put("include_lower",false);
            value.put("include_upper",false);
            range.put(values[0].trim(),value);
            meta.put("range",range);
        }else if (metaCondition.contains(Symbols.GTE)){
            values = metaCondition.split(Symbols.GTE);
            Map<String,Object> range = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("from",values[1].trim());
            value.put("to",null);
            value.put("include_lower",true);
            value.put("include_upper",false);
            range.put(values[0].trim(),value);
            meta.put("range",range);
        }else if (metaCondition.contains(MysqlKeyword.IS_NULL)){
            values = metaCondition.split(MysqlKeyword.IS_NULL);
            List<Map<String,Object>> mustNot = new ArrayList<>();

            Map<String,Object> term = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            value.put("field",values[0].trim());
            term.put("exists",value);
            mustNot.add(term);
            meta.put("must_not",mustNot);
        }else if (metaCondition.contains(MysqlKeyword.IS_NOT_NULL)){
            values = metaCondition.split(MysqlKeyword.IS_NOT_NULL);
            Map<String,Object> exists = new HashMap<>();
            exists.put("field",values[0].trim());
            meta.put("exists",exists);
        }else if (metaCondition.contains(MysqlKeyword.IN) && !metaCondition.contains(MysqlKeyword.NOT_IN)){
            values = metaCondition.split(MysqlKeyword.IN);
            String[] inValues = values[1].substring(values[1].indexOf(Symbols.LEFT_BRACKETS)+1,values[1].indexOf(Symbols.RIGHT_BRACKETS)).split(Symbols.COMMA);
            Map<String,Object> term = new HashMap<>();
            for (int index=0;index<inValues.length;index++) {
                inValues[index] = inValues[index].trim();
            }
            term.put(values[0].trim(),inValues);
            meta.put("terms",term);
        }else if (metaCondition.contains(MysqlKeyword.NOT_IN)){
            values = metaCondition.split(MysqlKeyword.NOT_IN);
            String[] inValues = values[1].substring(values[1].indexOf(Symbols.LEFT_BRACKETS)+1,values[1].indexOf(Symbols.RIGHT_BRACKETS)).split(Symbols.COMMA);
            List<Map<String,Object>> mustNot = new ArrayList<>();
            Map<String,Object> terms = new HashMap<>();
            Map<String,Object> keyAndValue = new HashMap<>();
            for (int index=0;index<inValues.length;index++) {
                inValues[index] = inValues[index].trim();
            }
            keyAndValue.put(values[0].trim(),inValues);
            terms.put("terms",keyAndValue);

            mustNot.add(terms);
            meta.put("must_not",mustNot);
        }else if (metaCondition.contains(MysqlKeyword.BETWEEN)) {

            values = metaCondition.split(MysqlKeyword.BETWEEN);
            Map<String,Object> range = new HashMap<>();
            Map<String,Object> value = new HashMap<>();
            String[] twoValues = values[1].split(MysqlKeyword.AND);
            value.put("from",twoValues[0].trim());
            value.put("to",twoValues[1].trim());
            value.put("include_lower",true);
            value.put("include_upper",true);
            range.put(values[0].trim(),value);
            meta.put("range",range);


        }else if (metaCondition.contains(MysqlKeyword.LIKE)) {
            Map<String,Object> wildcard = new HashMap<>();
            values = metaCondition.split(MysqlKeyword.LIKE);
            StringBuilder wildcardValue = new StringBuilder();
            String value = values[1];
            String likeValue = null;
            if (value.contains(Symbols.LIKE_LEFT)) {
                wildcardValue.append(Symbols.STAR);
                likeValue = value.substring(value.lastIndexOf(Symbols.LIKE_LEFT)+2);
            }else{
                likeValue = value.substring(2);
            }

            if (value.contains(Symbols.LIKE_RIGHT)) {
                likeValue = likeValue.substring(0,likeValue.lastIndexOf(Symbols.LIKE_RIGHT));
            }else {
                likeValue = likeValue.substring(0,likeValue.length()-2);
            }
            wildcardValue.append(likeValue);
            if (value.contains(Symbols.LIKE_RIGHT)) {
                wildcardValue.append(Symbols.STAR);
            }
            Map<String,Object> wildcardValueMap = new HashMap<>();
            wildcardValueMap.put("wildcard",wildcardValue.toString());

            wildcard.put(values[0].trim(),wildcardValueMap);
            meta.put("wildcard",wildcard);
        }
        return meta;
    }

    //所有关键字转大写,去掉组合关键字间的多余空格,关键字和符号没有空格的加空格,统一标准输出方便处理
    public static String formatCondition(String condition) {
        String newCondition = condition;

        newCondition = newCondition.replace(MysqlKeyword.IN+Symbols.LEFT_BRACKETS,MysqlKeyword.IN+Symbols.SPACE+Symbols.LEFT_BRACKETS);
        newCondition = newCondition.replace(MysqlKeyword.IN.toLowerCase()+Symbols.LEFT_BRACKETS,MysqlKeyword.IN.toLowerCase()+Symbols.SPACE+Symbols.LEFT_BRACKETS);
        newCondition = newCondition.replace(MysqlKeyword.LIKE+Symbols.SINGLE_QUOTATION_MARKS,MysqlKeyword.LIKE+Symbols.SPACE+Symbols.SINGLE_QUOTATION_MARKS);
        newCondition = newCondition.replace(MysqlKeyword.LIKE.toLowerCase()+Symbols.SINGLE_QUOTATION_MARKS,MysqlKeyword.LIKE.toLowerCase()+Symbols.SPACE+Symbols.SINGLE_QUOTATION_MARKS);

        while (newCondition.indexOf(Symbols.TWO_SPACE) > 0) {
            newCondition = newCondition.replace(Symbols.TWO_SPACE,Symbols.SPACE);
        }
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.DESC.toLowerCase()),splicingSpace(MysqlKeyword.DESC));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.ASC.toLowerCase()),splicingSpace(MysqlKeyword.ASC));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.BETWEEN.toLowerCase()),splicingSpace(MysqlKeyword.BETWEEN));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.IN.toLowerCase()),splicingSpace(MysqlKeyword.IN));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.NOT_IN.toLowerCase()),splicingSpace(MysqlKeyword.NOT_IN));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.IS_NULL.toLowerCase()),splicingSpace(MysqlKeyword.IS_NULL));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.IS_NOT_NULL.toLowerCase()),splicingSpace(MysqlKeyword.IS_NOT_NULL));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.IS_NOT_NULL.toLowerCase()),splicingSpace(MysqlKeyword.IS_NOT_NULL));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.AND.toLowerCase()), splicingSpace(MysqlKeyword.AND));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.OR.toLowerCase()),splicingSpace(MysqlKeyword.OR));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.ORDER_BY.toLowerCase()),splicingSpace(MysqlKeyword.ORDER_BY));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.GROUP_BY.toLowerCase()),splicingSpace(MysqlKeyword.GROUP_BY));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.SELECT.toLowerCase()),splicingSpace(MysqlKeyword.SELECT));
        newCondition = newCondition.replace(splicingSpace(MysqlKeyword.LIKE.toLowerCase()),splicingSpace(MysqlKeyword.LIKE));

        newCondition = newCondition.replace(Symbols.LEFT_BRACKETS+Symbols.SPACE,Symbols.LEFT_BRACKETS);
        newCondition = newCondition.replace(Symbols.SPACE+Symbols.RIGHT_BRACKETS,Symbols.RIGHT_BRACKETS);
        return newCondition;
    }

    //关键字加空格前后缀
    public static String splicingSpace(String keyWord){
        return Symbols.SPACE+keyWord+Symbols.SPACE;
    }


    //处理例如此类limit value1,value2
    private static void buildLimit(Map<String, Object> esQuery, String limitValues) {
        if (limitValues.contains(Symbols.COMMA)) {
            String[] limitValuesArray = limitValues.split(Symbols.COMMA);
            esQuery.put("from",limitValuesArray[0].trim());
            esQuery.put("size",limitValuesArray[1].trim());
        }else {
            esQuery.put("from",0);
            esQuery.put("size",limitValues.trim());
        }
    }

    //处理例如此类分组 field1,field2 having num > 1
    private static Map<String,Object> buildGroup(String groupByValues) {
        Map<String,Object> having = null;
        if (groupByValues.contains(MysqlKeyword.HAVING)) {
            String[] splitByHaving = groupByValues.split(MysqlKeyword.HAVING);
            String havingValue = splitByHaving[1];
            having = buildHaving(havingValue);
            groupByValues = splitByHaving[0].trim();
        }
        List<String> groupKeys = Arrays.asList(groupByValues.split(Symbols.COMMA));
        Map<String,Object> aggregations = new HashMap<>();
        Map<String,Object> groupby = new HashMap<>();
        Map<String,Object> composite = new HashMap<>();
        List<Map<String,Object>> sources = new ArrayList<>();
        for (String groupKey : groupKeys) {
            Map<String,Object> terms = new HashMap<>();
            Map<String,Object> termsValue = new HashMap<>();
            termsValue.put("field",groupKey.trim());
            terms.put("terms",termsValue);

            Map<String,Object> item = new HashMap<>();
            item.put(UUID.randomUUID().toString().toLowerCase().replace(Symbols.LINE,Symbols.EMPTY),terms);
            sources.add(item);
        }
        composite.put("sources",sources);
        groupby.put("composite",composite);
        if (Objects.nonNull(having)) {
            groupby.put("aggregations",having);
        }
        aggregations.put("groupby",groupby);
        return aggregations;
    }


    //处理例如此类分组条件  having num > 1
    private static Map<String,Object> buildHaving(String havingValue) {
        //只处理了常见的几种情况
        Map<String,Object> having = new HashMap<>();
        Map<String,Object> bucketSelector = new HashMap<>();
        Map<String,Object> bucketSelectorValue = new HashMap<>();
        Map<String,Object> bucketsPath = new HashMap<>();
        bucketsPath.put("a0","_count");
        bucketSelectorValue.put("buckets_path",bucketsPath);

        String source = null;
        Map<String,Object> params = new HashMap<>();
        if (havingValue.contains(Symbols.EQ) && !havingValue.contains(Symbols.LT) && !havingValue.contains(Symbols.GT)&&
                !havingValue.contains(Symbols.NEQ)) {
            source = esHavingSymbolReplace(Symbols.EQ_EN);
            params.put("v0",havingValue.split(Symbols.EQ)[1].trim());
        }else if (havingValue.contains(Symbols.NEQ)|| havingValue.contains(Symbols.NEQ2)){
            source = esHavingSymbolReplace(Symbols.NEQ_EN);
            params.put("v0",havingValue.split(Symbols.NEQ)[1].trim());
        }else if (havingValue.contains(Symbols.LT) && !havingValue.contains(Symbols.LTE)){
            source = esHavingSymbolReplace(Symbols.LT_EN);
            params.put("v0",havingValue.split(Symbols.LT)[1].trim());
        }else if (havingValue.contains(Symbols.LTE)){
            source = esHavingSymbolReplace(Symbols.LTE_EN);
            params.put("v0",havingValue.split(Symbols.LTE)[1].trim());
        }else if (havingValue.contains(Symbols.GT) && !havingValue.contains(Symbols.GTE)){
            source = esHavingSymbolReplace(Symbols.GT_EN);
            params.put("v0",havingValue.split(Symbols.GT)[1].trim());
        }else if (havingValue.contains(Symbols.GTE)){
            source = esHavingSymbolReplace(Symbols.GTE_EN);
            params.put("v0",havingValue.split(Symbols.GTE)[1].trim());
        }else if (havingValue.contains(MysqlKeyword.IS_NULL)){

        }else if (havingValue.contains(MysqlKeyword.IS_NOT_NULL)){

        }else if (havingValue.contains(MysqlKeyword.IN) && !havingValue.contains(MysqlKeyword.NOT_IN)){

        }else if (havingValue.contains(MysqlKeyword.NOT_IN)){

        }else if (havingValue.contains(MysqlKeyword.BETWEEN)) {

        }else if (havingValue.contains(MysqlKeyword.LIKE)) {

        }
        Map<String,Object> script = new HashMap<>();
        script.put("source",source);
        script.put("params",params);
        bucketSelectorValue.put("script",script);
        String bucketSelectorKey = "having."+UUID.randomUUID().toString().toLowerCase().replace(Symbols.LINE,Symbols.EMPTY);

        bucketSelector.put("bucket_selector",bucketSelectorValue);
        having.put(bucketSelectorKey,bucketSelector);
        return having;
    }

    private static String esHavingSymbolReplace(String targetSymbol) {
        return Symbols.ES_HAVING_SYMBOLS.replace(Symbols.ES_HAVING_PLACEHOLDER,targetSymbol.toLowerCase());
    }

    //处理例如此类排序 field1,field2 desc,field3,field4 asc
    private static  List<Map<String,Object>> buildSort(String orderByValues) {
        List<Map<String,Object>> sort = new ArrayList<>();
        boolean hasDesc = false;
        boolean hasAsc = false;

        if (orderByValues.contains(MysqlKeyword.DESC)){
            hasDesc = true;
        }
        if (orderByValues.contains(MysqlKeyword.ASC)){
            hasAsc = true;
        }
        if (StringUtils.countMatches(orderByValues,MysqlKeyword.ASC) > 1 || StringUtils.countMatches(orderByValues,MysqlKeyword.DESC) > 1) {
            throw new RuntimeException("not support same sort multi appear,optimization your sql");
        }
        if ( hasAsc && hasDesc) {
            int descIndex = orderByValues.indexOf(MysqlKeyword.DESC);
            int ascIndex = orderByValues.indexOf(MysqlKeyword.ASC);
            if ( descIndex > ascIndex ) {
                String[] splitByAscStrings = orderByValues.split(MysqlKeyword.ASC);
                dealAsc(splitByAscStrings[0]+MysqlKeyword.ASC,sort);
                dealDesc(splitByAscStrings[1].substring(splitByAscStrings[1].indexOf(Symbols.COMMA)),sort);
            }else{
                String[] splitByAscStrings = orderByValues.split(MysqlKeyword.DESC);
                dealDesc(splitByAscStrings[0]+MysqlKeyword.DESC,sort);
                dealAsc(splitByAscStrings[1].substring(splitByAscStrings[1].indexOf(Symbols.COMMA)),sort);
            }
        }

        if(hasAsc && !hasDesc) {
            dealAsc(orderByValues, sort);
        }
        if (!hasAsc && hasDesc) {
            dealDesc(orderByValues, sort);
        }
        return sort;
    }

    //处理降序
    private static void dealDesc(String orderByValues, List<Map<String, Object>> sort) {
        String orderByValuesTemp;
        List<String> sortKeys;
        orderByValuesTemp = orderByValues.substring(0,orderByValues.lastIndexOf(MysqlKeyword.DESC));
        sortKeys = Arrays.asList(orderByValuesTemp.split(Symbols.COMMA));
        for (String sortKey : sortKeys) {
            if (StringUtils.isBlank(sortKey)) {
                continue;
            }
            Map<String,Object> sortKeyMap = new HashMap<>();
            Map<String,Object> sortTypeMap = new HashMap<>();
            sortTypeMap.put("order",MysqlKeyword.DESC.toLowerCase());
            sortKeyMap.put(sortKey.trim(),sortTypeMap);
            sort.add(sortKeyMap);
        }
    }

    //处理升序
    private static void dealAsc(String orderByValues, List<Map<String, Object>> sort) {
        String orderByValuesTemp;
        List<String> sortKeys;
        orderByValuesTemp = orderByValues.substring(0,orderByValues.lastIndexOf(MysqlKeyword.ASC));
        sortKeys = Arrays.asList(orderByValuesTemp.split(Symbols.COMMA));
        for (String sortKey : sortKeys) {
            Map<String,Object> sortKeyMap = new HashMap<>();
            if (StringUtils.isBlank(sortKey)) {
                continue;
            }
            Map<String,Object> sortTypeMap = new HashMap<>();
            sortTypeMap.put("order",MysqlKeyword.ASC.toLowerCase());
            sortKeyMap.put(sortKey.trim(),sortTypeMap);
            sort.add(sortKeyMap);
        }
    }


    private static boolean isConditionHashFunction(String condition) {
        //todo 判断条件是否有函数，不支持函数
        return false;
    }

}
