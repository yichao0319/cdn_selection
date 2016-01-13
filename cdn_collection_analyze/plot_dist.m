%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Yi-Chao Chen @ UT Austin
%%
%% - Input:
%%
%%
%% - Output:
%%
%%
%% example:
%%
%%     
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

function plot_dist()
    
    %% --------------------
    %% DEBUG
    %% --------------------
    DEBUG0 = 0;
    DEBUG1 = 1;
    DEBUG2 = 1;  %% progress
    DEBUG3 = 1;  %% verbose
    DEBUG4 = 1;  %% results


    %% --------------------
    %% Constant
    %% --------------------
    input_dir = '../../data/cdn_collection_analyze/data/';
    fig_dir   = '../../data/cdn_collection_analyze/fig/';

    providers = {'wscdns', 'cdn20', 'chinacache', 'ccgslb', 'fastcdn', 'cloudcdn', 'yunjiasu-cdn', 'cctvcdn', 'jiashule', 'verycdn', 'cloudflare'};
    num_ips_cname_filename = 'num_ips_cname_dist.txt';
    num_ips_provider_filename = 'num_ips_provider_dist';

    colors   = {'r', 'b', [0 0.8 0], 'm', [1 0.85 0], [0 0 0.47], [0.45 0.17 0.48]};
    lines    = {'-', '--', '-.', ':'};
    markers  = {'+', 'o', '*', '.', 'x', 's', 'd', '^', '>', '<', 'p', 'h'};


    %% --------------------
    %% Variable
    %% --------------------
    fig_idx = 0;
    font_size = 20;
    

    %% --------------------
    %% Check input
    %% --------------------
    if nargin < 1, arg = 1; end


    %% --------------------
    %% Main starts
    %% --------------------
    
    %% Distribution of #IPs per CNAME
    filename = [input_dir num_ips_cname_filename];
    data = load(filename);
    [f,x] = ecdf(data);
    
    fig_idx = fig_idx + 1;
    fh = figure(fig_idx); clf;
    lh = plot(x, f, '-r');
    set(lh, 'LineWidth', 3);

    set(gca, 'FontSize', font_size);
    xlabel('#IPs / CNAME', 'FontSize', font_size);
    ylabel('CDF', 'FontSize', font_size);
    print(fh, '-depsc', [fig_dir 'num_ips_dist_cname.eps']);


    %% Distribution of #IPs per CNAME per Provider
    fig_idx = fig_idx + 1;
    fh = figure(fig_idx); clf;
    cnt = 0;
    for idx = 1:length(providers)
      provider = char(providers(idx));
      filename = [input_dir num_ips_provider_filename '.' provider '.txt'];

      data = load(filename);
      num_ips(idx) = sum(data);
      if length(data) < 5; continue; end

      cnt = cnt + 1;
      [f,x] = ecdf(data);
      lhs(cnt) = plot(x, f);
      hold on;
      set(lhs(cnt), 'Color', colors{mod(cnt-1,length(colors))+1});
      set(lhs(cnt), 'LineStyle', char(lines{mod(cnt-1,length(lines))+1}));
      set(lhs(cnt), 'LineWidth', 3);
      legends{cnt} = provider;
    end

    set(gca, 'FontSize', font_size);
    xlabel('#IPs / CNAME', 'FontSize', font_size);
    ylabel('CDF', 'FontSize', font_size);
    h = legend(lhs, legends, 'location', 'southeast');
    print(fh, '-depsc', [fig_dir 'num_ips_dist_provider.eps']);


    %% # unique IPs per Provider
    fig_idx = fig_idx + 1;
    fh = figure(fig_idx); clf;
    filename = [input_dir num_ips_provider_filename '.txt'];
    data = load(filename);
    [~,idx] = sort(data, 'descend');
    bh = bar(data(idx));
    sort_providers = providers(idx);

    set(gca, 'XLim', [0.5 length(providers)+0.5]);
    set(gca, 'XTick', [1:length(providers)]);
    set(gca, 'FontSize', font_size);
    % xlabel('#IPs / CNAME', 'FontSize', font_size);
    ylabel('# IPs', 'FontSize', font_size);

    set(gca, 'Position', [0.15 0.22 0.8 0.7]);

    %% rotate x tick
    set(gca, 'XTickLabel', ' ');
    XTick = get(gca, 'XTick');
    YLim = get(gca, 'YLim');
    % y = ones(length(XTick), 1) * (-30);
    y = ones(length(XTick), 1) * max(YLim) * (-0.02);
    x_label = [char(sort_providers(1))];
    for idx = 2:length(sort_providers)
      x_label = [x_label '|' char(sort_providers(idx))];
    end
    hText = text(XTick, y, x_label, 'fontsize', font_size);
    set(hText, 'Rotation', -45, 'HorizontalAlignment', 'left');
    print(fh, '-depsc', [fig_dir 'num_unique_ips_provider.eps']);


    %% # CNAMEs per Provider
    fig_idx = fig_idx + 1;
    fh = figure(fig_idx); clf;
    filename = [input_dir 'num_cnames_per_provider.txt'];
    data = load(filename);
    [~,idx] = sort(data, 'descend');
    bh = bar(data(idx));
    sort_providers = providers(idx);

    set(gca, 'XLim', [0.5 length(providers)+0.5]);
    set(gca, 'XTick', [1:length(providers)]);
    set(gca, 'FontSize', font_size);
    % xlabel('#IPs / CNAME', 'FontSize', font_size);
    ylabel('# CNAMEs', 'FontSize', font_size);

    set(gca, 'Position', [0.15 0.22 0.8 0.7]);

    %% rotate x tick
    set(gca, 'XTickLabel', ' ');
    XTick = get(gca, 'XTick');
    YLim = get(gca, 'YLim');
    y = ones(length(XTick), 1) * max(YLim) * (-0.02);
    x_label = [char(sort_providers(1))];
    for idx = 2:length(sort_providers)
      x_label = [x_label '|' char(sort_providers(idx))];
    end
    hText = text(XTick, y, x_label, 'fontsize', font_size);
    set(hText, 'Rotation', -45, 'HorizontalAlignment', 'left');
    print(fh, '-depsc', [fig_dir 'num_cnames_per_provider.eps']);
end
